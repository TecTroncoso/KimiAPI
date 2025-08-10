import asyncio
import httpx
import random
import time
import json
import threading
import uuid
from typing import Dict, List, Optional, AsyncGenerator

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

# --- KimiClient Class (Versión de Producción) ---

class KimiClient:
    CHAT_INACTIVITY_TIMEOUT = 30 * 60

    def __init__(self, debug=False, pool_size: int = 5): # Debug desactivado por defecto
        self.device_id = str(int(time.time() * 1000)) + str(random.randint(100000000, 999999999))
        self.session_id = str(int(time.time() * 1000)) + str(random.randint(100000000, 999999999))
        self.auth_token = None
        self.debug = debug
        self.chats = {}
        self.chat_pool = []
        self.active_chats = {}
        self.pool_size = pool_size
        self.lock = threading.Lock()
        self.deleted_chats = set()
        
        self.async_client = httpx.AsyncClient(timeout=60.0)
        
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "origin": "https://www.kimi.com",
            "referer": "https://www.kimi.com/",
            "r-timezone": "Europe/Paris",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "x-language": "zh-CN",
            "x-msh-device-id": self.device_id,
            "x-msh-platform": "web",
            "x-msh-session-id": self.session_id,
            "x-traffic-id": self.device_id
        }
        
    async def initialize(self):
        print("Authenticating and initializing chat pool...")
        await self._initialize_chat_pool()
        print(f"Initialization complete. Pool status: {self.get_pool_status()}")
        self.stop_event = threading.Event()
        self.cleanup_thread = threading.Thread(target=self._periodic_cleanup_task, daemon=True)
        self.cleanup_thread.start()
        print(f"Session cleanup thread started. Timeout set to {self.CHAT_INACTIVITY_TIMEOUT} seconds.")

    def _periodic_cleanup_task(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        while not self.stop_event.is_set():
            time.sleep(60)
            inactive_users = []
            with self.lock:
                for user_id, session_data in self.active_chats.copy().items():
                    if time.time() - session_data["last_used"] > self.CHAT_INACTIVITY_TIMEOUT:
                        inactive_users.append(user_id)
            
            if inactive_users:
                if self.debug: print(f"[DEBUG] Cleanup thread found inactive users: {inactive_users}")
                for user_id in inactive_users:
                    loop.run_until_complete(self.release_chat_from_user(user_id))
        
        loop.close()

    async def _initialize_chat_pool(self):
        if self.debug: print(f"[DEBUG] Initializing chat pool with {self.pool_size} chats...")
        await self.authenticate()
        tasks = [self._create_and_add_to_pool(i) for i in range(self.pool_size)]
        await asyncio.gather(*tasks)

    async def _create_and_add_to_pool(self, index: int):
        try:
            chat_id = await self.create_chat(f"Preloaded Chat #{index+1}")
            with self.lock:
                self.chat_pool.append({"id": chat_id, "status": "available", "name": f"Preloaded Chat #{index+1}", "created_at": time.time(), "last_used": None})
            if self.debug: print(f"[DEBUG] Added preloaded chat {chat_id} to pool")
        except Exception as e:
            if self.debug: print(f"[DEBUG] Failed to create preloaded chat #{index+1}: {e}")

    async def authenticate(self):
        if self.auth_token: return self.auth_token
        if self.debug: print("[DEBUG] Authenticating...")
        response = await self.async_client.post("https://www.kimi.com/api/device/register", json={}, headers=self.headers)
        response.raise_for_status()
        self.auth_token = response.cookies.get("kimi-auth")
        if not self.auth_token: raise Exception("Authentication failed: No auth token received.")
        if self.debug: print(f"[DEBUG] Retrieved auth_token: [REDACTED]")
        return self.auth_token

    async def create_chat(self, name="未命名会话"):
        if not self.auth_token: await self.authenticate()
        headers = {**self.headers, "authorization": f"Bearer {self.auth_token}", "cookie": f"kimi-auth={self.auth_token}"}
        payload = {"born_from": "home", "is_example": False, "kimiplus_id": "kimi", "name": name, "source": "web", "tags": []}
        response = await self.async_client.post("https://www.kimi.com/api/chat", json=payload, headers=headers)
        response.raise_for_status()
        chat_id = response.json()["id"]
        with self.lock: self.chats[chat_id] = name
        if self.debug: print(f"[DEBUG] Created new chat_id: {chat_id}")
        return chat_id
        
    def _get_available_chat_from_pool(self) -> Optional[Dict]:
        with self.lock:
            for chat in self.chat_pool:
                if chat["status"] == "available":
                    chat["status"] = "in_use"
                    chat["last_used"] = time.time()
                    return chat
            return None

    async def get_or_assign_chat_for_user(self, user_id: str) -> str:
        with self.lock:
            if user_id in self.active_chats:
                session_data = self.active_chats[user_id]
                session_data["last_used"] = time.time()
                chat_id = session_data["chat_id"]
                if self.debug: print(f"[DEBUG] Reusing existing chat {chat_id} for user {user_id}. Timer reset.")
                return chat_id
        
        chat = self._get_available_chat_from_pool()
        if chat is None:
            chat_id = await self.create_chat(f"Chat for {user_id}")
            if self.debug: print(f"[DEBUG] No available chat in pool, created new: {chat_id} for user {user_id}")
        else:
            chat_id = chat["id"]
            if self.debug: print(f"[DEBUG] Assigned chat {chat_id} from pool to user {user_id}")
        
        with self.lock: self.active_chats[user_id] = {"chat_id": chat_id, "last_used": time.time()}
        return chat_id

    async def release_chat_from_user(self, user_id: str):
        chat_id_to_delete = None
        with self.lock:
            if user_id in self.active_chats:
                chat_id_to_delete = self.active_chats[user_id]["chat_id"]
                del self.active_chats[user_id]
                for i, pool_chat in enumerate(self.chat_pool):
                    if pool_chat["id"] == chat_id_to_delete: self.chat_pool.pop(i); break
        
        if chat_id_to_delete:
            try: await self._delete_chat(chat_id_to_delete)
            except Exception as e:
                if self.debug: print(f"[DEBUG] Failed to delete chat {chat_id_to_delete}: {e}")
            await self._create_and_add_to_pool(0)
            if self.debug: print(f"[DEBUG] Released and deleted inactive chat {chat_id_to_delete} for user {user_id}.")

    async def _delete_chat(self, chat_id: str):
        if not self.auth_token: await self.authenticate()
        headers = {**self.headers, "authorization": f"Bearer {self.auth_token}", "cookie": f"kimi-auth={self.auth_token}"}
        response = await self.async_client.delete(f"https://www.kimi.com/api/chat/{chat_id}", headers=headers)
        if self.debug: print(f"[DEBUG] Delete chat {chat_id} response status: {response.status_code}")
        response.raise_for_status()
        with self.lock:
            if chat_id in self.chats: del self.chats[chat_id]
            self.deleted_chats.add(chat_id)

    async def send_message_stream(self, chat_id: str, message: str, model="k2") -> AsyncGenerator[Dict, None]:
        headers = {**self.headers, "authorization": f"Bearer {self.auth_token}", "cookie": f"kimi-auth={self.auth_token}", "accept": "text/event-stream"}
        payload = {"messages": [{"role": "user", "content": message}], "model": model, "kimiplus_id": "kimi", "use_search": True, "refs": [], "history": []}
        
        try:
            async with self.async_client.stream("POST", f"https://www.kimi.com/api/chat/{chat_id}/completion/stream", json=payload, headers=headers, timeout=600) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if line and line.startswith("data: "):
                        try: yield json.loads(line[6:])
                        except json.JSONDecodeError:
                            if self.debug: print(f"[DEBUG] JSON decode error for line: {line}")
                            continue
        except httpx.HTTPStatusError as e:
            print(f"[ERROR] HTTP Status Error when calling Kimi API: {e.response.status_code}")
            try:
                error_response_text = await e.response.aread()
                print(f"[ERROR] Kimi API Error Response Body: {error_response_text.decode()}")
            except Exception as read_exc:
                print(f"[ERROR] Could not read error response body: {read_exc}")
            raise
    
    def get_pool_status(self) -> Dict:
        with self.lock:
            available_count = sum(1 for chat in self.chat_pool if chat["status"] == "available")
            active_users_details = {user: data["chat_id"] for user, data in self.active_chats.items()}
            return {"pool_target_size": self.pool_size, "current_pool_size": len(self.chat_pool), "available_in_pool": available_count, "active_users_count": len(self.active_chats), "active_users_details": active_users_details, "total_deleted_chats": len(self.deleted_chats)}

# --- FastAPI App & OpenAI-Compatible Models ---
app = FastAPI(title="Kimi API with Automatic Session Timeout", version="3.2.0")
kimi_client = KimiClient(debug=False, pool_size=5) # debug=False

@app.on_event("startup")
async def startup_event():
    await kimi_client.initialize()

# (Modelos Pydantic sin cambios)
class ModelCard(BaseModel): id: str; object: str = "model"; created: int = Field(default_factory=lambda: int(time.time())); owned_by: str = "kimi"
class ModelList(BaseModel): object: str = "list"; data: List[ModelCard]
class ChatMessage(BaseModel): role: str; content: str
class ChatCompletionRequest(BaseModel): model: str = "k2"; messages: List[ChatMessage]; stream: bool = False; user: Optional[str] = None
class ChatCompletionResponseChoice(BaseModel): index: int; message: ChatMessage; finish_reason: str = "stop"
class ChatCompletionResponse(BaseModel): id: str = Field(default_factory=lambda: f"chatcmpl-{uuid.uuid4().hex}"); object: str = "chat.completion"; created: int = Field(default_factory=lambda: int(time.time())); model: str; choices: List[ChatCompletionResponseChoice]; user_id: str; usage: Dict = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
class DeltaMessage(BaseModel): role: Optional[str] = None; content: Optional[str] = None
class ChatCompletionResponseStreamChoice(BaseModel): index: int; delta: DeltaMessage; finish_reason: Optional[str] = None
class ChatCompletionStreamResponse(BaseModel): id: str = Field(default_factory=lambda: f"chatcmpl-{uuid.uuid4().hex}"); object: str = "chat.completion.chunk"; created: int = Field(default_factory=lambda: int(time.time())); model: str; user_id: Optional[str] = None; choices: List[ChatCompletionResponseStreamChoice]

# --- API Endpoints ---
@app.get("/v1/models", response_model=ModelList, tags=["Chat"])
async def list_models(): return ModelList(data=[ModelCard(id="k2")])

@app.post("/v1/chat/completions", tags=["Chat"])
async def create_chat_completion(request: ChatCompletionRequest):
    if request.user: user_id = request.user
    else: user_id = f"session-{uuid.uuid4().hex}"
    
    try:
        chat_id = await kimi_client.get_or_assign_chat_for_user(user_id)
        last_user_message = next((m.content for m in reversed(request.messages) if m.role == 'user'), None)
        if not last_user_message: raise HTTPException(status_code=400, detail="No user message found.")

        if request.stream:
            async def stream_generator():
                completion_id = f"chatcmpl-{uuid.uuid4().hex}"; created_time = int(time.time())
                first_chunk = ChatCompletionStreamResponse(id=completion_id, created=created_time, model=request.model, user_id=user_id, choices=[ChatCompletionResponseStreamChoice(index=0, delta=DeltaMessage(role="assistant"))])
                yield f"data: {first_chunk.json()}\n\n"
                try:
                    async for kimi_event in kimi_client.send_message_stream(chat_id, last_user_message, request.model):
                        if kimi_event.get("event") == "cmpl":
                            text_chunk = kimi_event.get("text", "")
                            if text_chunk:
                                chunk = ChatCompletionStreamResponse(id=completion_id, created=created_time, model=request.model, choices=[ChatCompletionResponseStreamChoice(index=0, delta=DeltaMessage(content=text_chunk))])
                                yield f"data: {chunk.json()}\n\n"
                finally:
                    final_chunk = ChatCompletionStreamResponse(id=completion_id, created=created_time, model=request.model, choices=[ChatCompletionResponseStreamChoice(index=0, delta=DeltaMessage(), finish_reason="stop")])
                    yield f"data: {final_chunk.json()}\n\n"
                    yield "data: [DONE]\n\n"
            return StreamingResponse(stream_generator(), media_type="text/event-stream")
        else:
            full_response_text = ""
            async for kimi_event in kimi_client.send_message_stream(chat_id, last_user_message, request.model):
                if kimi_event.get("event") == "cmpl":
                    full_response_text += kimi_event.get("text", "")
            return ChatCompletionResponse(model=request.model, user_id=user_id, choices=[ChatCompletionResponseChoice(index=0, message=ChatMessage(role="assistant", content=full_response_text))])
    
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"Failed to get response from Kimi API. Status: {e.response.status_code}. Body: {e.response.text}")
    except Exception as e:
        print(f"[FATAL ERROR] An unexpected error occurred in endpoint: {e}")
        raise HTTPException(status_code=500, detail="An internal server error occurred.")

@app.get("/pool/status", tags=["Admin"])
async def get_pool_status(): return kimi_client.get_pool_status()

@app.get("/health", tags=["Admin"])
async def health_check(): return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
