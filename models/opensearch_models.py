from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class IndexInfo(BaseModel):
    """OpenSearch Index Info"""
    index: str
    doc_count: int
    status: str
    health: str

class CreateIndexRequest(BaseModel):
    """Create Index Request"""
    index: str

class GenericResponse(BaseModel):
    """Generic Response"""
    success: bool
    message: str

class SearchRequest(BaseModel):
    """Search Request"""
    index: str
    query: str

class SearchHit(BaseModel):
    """Search Hit"""
    id: str
    index: str
    score: float
    source: Dict[str, Any]

class SearchResponse(BaseModel):
    """Search Response"""
    hits: List[SearchHit]

class ErrorResponse(BaseModel):
    """Error Response"""
    error: str 