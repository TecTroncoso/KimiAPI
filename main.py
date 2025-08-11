import asyncio
import httpx
import random
import time
import json
import uuid
from typing import Dict, List, Optional, AsyncGenerator

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

# --- KimiClient Class (Versión Optimizada para Baja Latencia) ---

class KimiClient:
    CHAT_INACTIVITY_TIMEOUT = 30 * 60  # 30 minutos

    def __init__(self, debug=False, pool_size: int = 5):
        self.debug = debug
        self.pool_target_size = pool_size
        
        # Identificadores de sesión y dispositivo
        self.device_id = f"{int(time.time() * 1000)}{random.randint(100000000, 999999999)}"
        self.session_id = f"{int(time.time() * 1000)}{random.randint(100000000, 999999999)}"
        
        self.auth_token: Optional[str] = None
        
        # --- Optimizaciones Clave ---
        # 1. Usar asyncio.Queue para un pool sin bloqueos y eficiente.
        self.chat_pool: asyncio.Queue[Dict] = asyncio.Queue(maxsize=pool_size * 2) # Margen extra
        
        # 2. Usar asyncio.Lock en lugar de threading.Lock para un entorno 100% async.
        self.lock = asyncio.Lock()
        
        # 3. Estructuras de datos para seguimiento
        self.active_chats: Dict[str, Dict] = {} # {user_id: {"chat_id": ..., "last_used": ...}}
        self.deleted_chats_count = 0

        # Cliente HTTP asíncrono
        self.async_client = httpx.AsyncClient(timeout=60.0)
        self.headers = self._get_base_headers()

        # Tareas en segundo plano
        self.cleanup_task: Optional[asyncio.Task] = None
        self.replenisher_task: Optional[asyncio.Task] = None

    def _get_base_headers(self) -> Dict[str, str]:
        return {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "origin": "https://www.kimi.com",
            "referer": "https://www.kimi.com/",
            "r-timezone": "Europe/Paris",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "x-language": "zh-CN",
            "x-msh-device-id": self.device_id,
            "x-msh-platform": "web",
            "x-msh-session-id": self.session_id,
            "x-traffic-id": self.device_id
        }

    async def initialize(self):
        """Inicializa la autenticación y las tareas en segundo plano."""
        print("Authenticating and starting background tasks...")
        await self.authenticate()
        
        # Iniciar las tareas en segundo plano para gestionar el pool y la limpieza de sesiones
        self.cleanup_task = asyncio.create_task(self._periodic_cleanup_task())
        self.replenisher_task = asyncio.create_task(self._pool_replenisher_task())
        
        print(f"Initialization complete. Pool Target: {self.pool_target_size}, Inactivity Timeout: {self.CHAT_INACTIVITY_TIMEOUT}s")

    async def close(self):
        """Cierra el cliente y cancela las tareas de forma segura."""
        print("Closing KimiClient and cleaning up resources...")
        if self.cleanup_task: self.cleanup_task.cancel()
        if self.replenisher_task: self.replenisher_task.cancel()
        # Esperar a que las tareas se cancelen
        await asyncio.gather(self.cleanup_task, self.replenisher_task, return_exceptions=True)
        await self.async_client.aclose()
        print("Client closed.")

    async def _periodic_cleanup_task(self):
        """Tarea asíncrona que limpia sesiones de usuario inactivas periódicamente."""
        while True:
            await asyncio.sleep(60)
            now = time.time()
            inactive_users = []
            
            # Es seguro iterar sin bloqueo, la modificación se hará bajo bloqueo
            for user_id, session_data in self.active_chats.items():
                if now - session_data["last_used"] > self.CHAT_INACTIVITY_TIMEOUT:
                    inactive_users.append(user_id)
            
            if inactive_users:
                if self.debug: print(f"[DEBUG][CLEANUP] Found inactive users: {inactive_users}")
                # Liberar todos los chats inactivos concurrentemente
                await asyncio.gather(*(self.release_chat_from_user(user_id) for user_id in inactive_users))

    async def _pool_replenisher_task(self):
        """Tarea asíncrona que se asegura de que el pool de chats siempre esté lleno."""
        while True:
            try:
                # Calcula cuántos chats faltan para llegar al tamaño objetivo
                # (Considera los que están en el pool y los que están en uso)
                current_total_chats = self.chat_pool.qsize() + len(self.active_chats)
                needed = self.pool_target_size - current_total_chats

                if needed > 0:
                    if self.debug: print(f"[DEBUG][REPLENISH] Pool needs {needed} chats. Current pool size: {self.chat_pool.qsize()}, Active: {len(self.active_chats)}")
                    # Crea los chats que faltan en paralelo
                    tasks = [self._create_and_add_to_pool() for _ in range(needed)]
                    await asyncio.gather(*tasks)
                
                # Espera un poco antes de volver a comprobar para no saturar
                await asyncio.sleep(1) 
            except Exception as e:
                print(f"[ERROR][REPLENISH] Replenisher task failed: {e}")
                await asyncio.sleep(10) # Espera más tiempo si hay un error

    async def _create_and_add_to_pool(self):
        """Crea un nuevo chat y lo añade a la cola del pool."""
        try:
            chat_id = await self._create_chat_on_api()
            chat_info = {"id": chat_id, "created_at": time.time()}
            await self.chat_pool.put(chat_info)
            if self.debug: print(f"[DEBUG][POOL] Added new chat {chat_id} to pool. Pool size: {self.chat_pool.qsize()}")
        except Exception as e:
            print(f"[ERROR][POOL] Failed to create and add chat to pool: {e}")

    async def authenticate(self):
        if self.auth_token: return
        if self.debug: print("[DEBUG] Authenticating...")
        response = await self.async_client.post("https://www.kimi.com/api/device/register", json={}, headers=self.headers)
        response.raise_for_status()
        self.auth_token = response.cookies.get("kimi-auth")
        if not self.auth_token: raise Exception("Authentication failed: No auth token received.")
        if self.debug: print(f"[DEBUG] Authentication successful.")
        return self.auth_token

    async def _create_chat_on_api(self, name="Preloaded Chat"):
        """Llamada a la API para crear un chat. Función interna."""
        await self.authenticate() # Asegurarse de estar autenticado
        headers = {**self.headers, "authorization": f"Bearer {self.auth_token}", "cookie": f"kimi-auth={self.auth_token}"}
        payload = {"born_from": "home", "is_example": False, "kimiplus_id": "kimi", "name": name, "source": "web", "tags": []}
        response = await self.async_client.post("https://www.kimi.com/api/chat", json=payload, headers=headers)
        response.raise_for_status()
        return response.json()["id"]

    async def get_or_assign_chat_for_user(self, user_id: str) -> str:
        """Asigna un chat a un usuario, de forma extremadamente rápida."""
        async with self.lock:
            if user_id in self.active_chats:
                session_data = self.active_chats[user_id]
                session_data["last_used"] = time.time()
                chat_id = session_data["chat_id"]
                if self.debug: print(f"[DEBUG][ASSIGN] Reusing chat {chat_id} for user {user_id}. Timer reset.")
                return chat_id

        # --- Ruta crítica de baja latencia ---
        try:
            # Intenta coger un chat del pool sin esperar. Es casi instantáneo.
            chat = self.chat_pool.get_nowait()
            chat_id = chat["id"]
            if self.debug: print(f"[DEBUG][ASSIGN] Assigned chat {chat_id} from pool to user {user_id}")
        except asyncio.QueueEmpty:
            # Si el pool está vacío (caso excepcional), crea uno sobre la marcha.
            # La tarea replenisher debería evitar que esto ocurra a menudo.
            print(f"[WARNING][ASSIGN] Pool empty! Creating emergency chat for user {user_id}.")
            chat_id = await self._create_chat_on_api(f"Emergency Chat for {user_id}")
        
        async with self.lock:
            self.active_chats[user_id] = {"chat_id": chat_id, "last_used": time.time()}
        
        return chat_id

    async def release_chat_from_user(self, user_id: str):
        """Libera un chat de un usuario y lo elimina de la API."""
        chat_id_to_delete = None
        async with self.lock:
            if user_id in self.active_chats:
                chat_id_to_delete = self.active_chats.pop(user_id)["chat_id"]
                self.deleted_chats_count += 1
        
        if chat_id_to_delete:
            try:
                await self._delete_chat_on_api(chat_id_to_delete)
                if self.debug: print(f"[DEBUG][RELEASE] Released and deleted chat {chat_id_to_delete} from user {user_id}.")
            except Exception as e:
                print(f"[ERROR][RELEASE] Failed to delete chat {chat_id_to_delete}: {e}")
        # No es necesario añadir un nuevo chat aquí. La tarea replenisher se encargará.

    async def _delete_chat_on_api(self, chat_id: str):
        """Llamada a la API para eliminar un chat."""
        await self.authenticate()
        headers = {**self.headers, "authorization": f"Bearer {self.auth_token}", "cookie": f"kimi-auth={self.auth_token}"}
        response = await self.async_client.delete(f"https://www.kimi.com/api/chat/{chat_id}", headers=headers)
        if self.debug and response.status_code != 200: 
            print(f"[DEBUG] Delete chat {chat_id} response status: {response.status_code}")
        response.raise_for_status()

    async def send_message_stream(self, chat_id: str, message: str, model="k2") -> AsyncGenerator[Dict, None]:
        headers = {**self.headers, "authorization": f"Bearer {self.auth_token}", "cookie": f"kimi-auth={self.auth_token}", "accept": "text/event-stream"}
        payload = {"messages": [{"role": "user", "content": message}], "model": model, "kimiplus_id": "kimi", "use_search": True, "refs": [], "history": []}
        
        async with self.async_client.stream("POST", f"https://www.kimi.com/api/chat/{chat_id}/completion/stream", json=payload, headers=headers, timeout=600) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    try:
                        yield json.loads(line[6:])
                    except json.JSONDecodeError:
                        if self.debug: print(f"[DEBUG] JSON decode error for line: {line}")

    def get_pool_status(self) -> Dict:
        # No necesitamos bloqueo para leer valores atómicos como qsize y len
        return {
            "pool_target_size": self.pool_target_size,
            "current_pool_size": self.chat_pool.qsize(),
            "active_users_count": len(self.active_chats),
            "active_users_details": {user: data["chat_id"] for user, data in self.active_chats.items()},
            "total_deleted_chats": self.deleted_chats_count
        }

# --- FastAPI App & OpenAI-Compatible Models ---
app = FastAPI(title="Kimi API - Latency Optimized", version="4.0.0")
kimi_client = KimiClient(debug=False, pool_size=5)

@app.on_event("startup")
async def startup_event():
    await kimi_client.initialize()

@app.on_event("shutdown")
async def shutdown_event():
    await kimi_client.close()

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
async def list_models():
    return ModelList(data=[ModelCard(id="k2")])

@app.post("/v1/chat/completions", tags=["Chat"])
async def create_chat_completion(request: ChatCompletionRequest):
    user_id = request.user if request.user else f"session-{uuid.uuid4().hex}"
    
    try:
        chat_id = await kimi_client.get_or_assign_chat_for_user(user_id)
        last_user_message = next((m.content for m in reversed(request.messages) if m.role == 'user'), None)
        if not last_user_message:
            raise HTTPException(status_code=400, detail="No user message found.")

        if request.stream:
            async def stream_generator():
                completion_id = f"chatcmpl-{uuid.uuid4().hex}"
                created_time = int(time.time())
                # Enviar el primer chunk con el rol del asistente
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
                    # Enviar el chunk final para indicar que la transmisión ha terminado
                    final_chunk = ChatCompletionStreamResponse(id=completion_id, created=created_time, model=request.model, choices=[ChatCompletionResponseStreamChoice(index=0, delta=DeltaMessage(), finish_reason="stop")])
                    yield f"data: {final_chunk.json()}\n\n"
                    yield "data: [DONE]\n\n"
            
            return StreamingResponse(stream_generator(), media_type="text/event-stream")
        else: # Non-streaming
            full_response_text = ""
            async for kimi_event in kimi_client.send_message_stream(chat_id, last_user_message, request.model):
                if kimi_event.get("event") == "cmpl":
                    full_response_text += kimi_event.get("text", "")
            
            return ChatCompletionResponse(model=request.model, user_id=user_id, choices=[ChatCompletionResponseChoice(index=0, message=ChatMessage(role="assistant", content=full_response_text))])
    
    except httpx.HTTPStatusError as e:
        error_body = e.response.text
        print(f"[FATAL ERROR] Kimi API HTTP Error: {e.response.status_code} - Body: {error_body}")
        raise HTTPException(status_code=502, detail=f"Failed to get response from Kimi API. Status: {e.response.status_code}. Body: {error_body}")
    except Exception as e:
        print(f"[FATAL ERROR] An unexpected error occurred in endpoint: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An internal server error occurred.")

@app.get("/pool/status", tags=["Admin"])
async def get_pool_status():
    return kimi_client.get_pool_status()

@app.get("/health", tags=["Admin"])
async def health_check():
    return {"status": "healthy"}

# El bloque if __name__ == "__main__": no se necesita si ejecutas con `uvicorn main:app`
# pero se mantiene por si se ejecuta el script directamente.
if __name__ == "__main__":
    import uvicorn
    # reload=False es recomendado para producción para evitar reinicios inesperados.
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True) 
