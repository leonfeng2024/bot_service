from pydantic import BaseModel
from typing import List

class ChatRequest(BaseModel):
    username: str
    query: str

class DatabaseSchemaRequest(BaseModel):
    """Request model for importing database schema into Neo4j."""
    schemas: List[str]
    description: str = "Database schema import request"