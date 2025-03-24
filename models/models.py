from pydantic import BaseModel
from typing import List

class ChatRequest(BaseModel):
    username: str
    query: str
    uuid: str

class TokenData(BaseModel):
    user_id: int
    role: str
    uuid: str
    exp: int

class LogoutRequest(BaseModel):
    uuid: str

class DatabaseSchemaRequest(BaseModel):
    """Request model for importing database schema into Neo4j."""
    schemas: List[str]
    description: str = "Database schema import request"