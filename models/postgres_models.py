from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class TableInfo(BaseModel):
    """PostgreSQL表信息"""
    table_name: str
    table_schema: str
    table_type: str

class ExecuteQueryRequest(BaseModel):
    """执行SQL查询请求模型"""
    query: str

class ExecuteQueryResponse(BaseModel):
    """SQL查询结果响应模型"""
    rows: List[Dict[str, Any]]

class ErrorResponse(BaseModel):
    """错误响应模型"""
    error: str

class ImportResponse(BaseModel):
    """导入响应模型"""
    success: bool
    message: str
    tables_affected: Optional[int] = None 