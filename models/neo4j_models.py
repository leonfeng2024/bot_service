from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional


class Neo4jDatabaseInfo(BaseModel):
    """Model for Neo4j database information"""
    name: str
    address: str
    role: str
    status: str
    default: bool


class Neo4jNode(BaseModel):
    """Model for Neo4j node"""
    id: str
    labels: List[str]
    properties: Dict[str, Any]


class Neo4jRelationship(BaseModel):
    """Model for Neo4j relationship"""
    id: str
    type: str
    startNodeId: str
    endNodeId: str
    properties: Dict[str, Any]


class Neo4jGraphData(BaseModel):
    """Model for Neo4j graph data (nodes and relationships)"""
    nodes: List[Neo4jNode]
    relationships: List[Neo4jRelationship]


class ExecuteQueryRequest(BaseModel):
    """Request model for executing a Neo4j query"""
    database: str
    query: str


class ExecuteQueriesRequest(BaseModel):
    """Request model for executing multiple Neo4j queries"""
    database: str
    queries: List[str]


class ExecuteQueriesResponse(BaseModel):
    """Response model for executing multiple Neo4j queries"""
    created: int
    elapsed: int
    status: str 