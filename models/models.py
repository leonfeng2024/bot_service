from pydantic import BaseModel

class ChatRequest(BaseModel):
    username: str
    query: str