from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class IndexInfo(BaseModel):
    """OpenSearch索引信息"""
    index: str
    doc_count: int
    status: str
    health: str

class CreateIndexRequest(BaseModel):
    """创建索引请求模型"""
    index: str

class GenericResponse(BaseModel):
    """通用响应模型"""
    success: bool
    message: str

class SearchRequest(BaseModel):
    """搜索请求模型"""
    index: str
    query: str

class SearchHit(BaseModel):
    """搜索结果命中项"""
    id: str
    index: str
    score: float
    source: Dict[str, Any]

class SearchResponse(BaseModel):
    """搜索响应模型"""
    hits: List[SearchHit]

class ErrorResponse(BaseModel):
    """错误响应模型"""
    error: str 