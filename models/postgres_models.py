from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class TableInfo(BaseModel):
    """PostgreSQL table info"""
    table_name: str
    table_schema: str
    table_type: str

class ExecuteQueryRequest(BaseModel):
    """Execute SQL Query Request"""
    query: str

class ExecuteQueryResponse(BaseModel):
    """SQL Query Result Response"""
    rows: List[Dict[str, Any]]

class ErrorResponse(BaseModel):
    """Error Response"""
    error: str

class ImportResponse(BaseModel):
    """Import Response"""
    success: bool
    message: str
    tables_affected: Optional[int] = None 