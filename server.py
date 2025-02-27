from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from service.chat_service import ChatService
from service.llm_service import LLMService
from service.embedding_service import EmbeddingService
from models.models import ChatRequest

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

chat_service = ChatService()
llm_service = LLMService()
# embedding_service = EmbeddingService()

@app.on_event("startup")
async def startup_event():
    pass

@app.post("/chat")
async def chat(request: ChatRequest):
    return await chat_service.handle_chat(request.username, request.query)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
