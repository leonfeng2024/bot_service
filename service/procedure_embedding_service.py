import os
import re
import asyncio
from typing import List, Dict, Any
import json
from opensearchpy import OpenSearch, helpers
from service.embedding_service import EmbeddingService

# OpenSearch connection configuration
OPENSEARCH_HOST = "localhost"
OPENSEARCH_PORT = 9200
OPENSEARCH_USER = "admin"
OPENSEARCH_PASSWORD = "admin"
# Set to False if your OpenSearch doesn't use SSL
USE_SSL = False

# Index name for procedures
PROCEDURE_INDEX = "procedure_index"

def connect_to_opensearch() -> OpenSearch:
    """Create and return an OpenSearch client"""
    try:
        client = OpenSearch(
            hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
            http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
            use_ssl=USE_SSL,
            verify_certs=False,
            ssl_show_warn=False,
            timeout=30
        )
        # Test connection
        client.info()
        print("Successfully connected to OpenSearch")
        return client
    except Exception as e:
        print(f"Error connecting to OpenSearch: {str(e)}")
        print("Make sure OpenSearch is running and configuration is correct.")
        raise

async def create_procedure_index(client: OpenSearch, dimension: int) -> None:
    """Create the procedure index with KNN mapping if it doesn't exist"""
    try:
        # Delete the index if it exists with wrong dimension
        if client.indices.exists(index=PROCEDURE_INDEX):
            # Get the mapping
            mapping = client.indices.get_mapping(index=PROCEDURE_INDEX)
            
            # Check vector dimension in the mapping
            if PROCEDURE_INDEX in mapping:
                props = mapping[PROCEDURE_INDEX].get('mappings', {}).get('properties', {})
                sql_embedding = props.get('sql_embedding', {})
                
                if sql_embedding and sql_embedding.get('type') == 'knn_vector':
                    current_dim = sql_embedding.get('dimension')
                    
                    # If dimensions don't match, delete the index to recreate
                    if current_dim != dimension:
                        print(f"Index {PROCEDURE_INDEX} has dimension {current_dim}, but need {dimension}")
                        client.indices.delete(index=PROCEDURE_INDEX)
                        print(f"Deleted index {PROCEDURE_INDEX} to recreate with correct dimension")
                    else:
                        print(f"Index {PROCEDURE_INDEX} already has correct dimension {dimension}")
                        return
        
        if not client.indices.exists(index=PROCEDURE_INDEX):
            mapping = {
                "settings": {
                    "index": {
                        "knn": True,
                        "knn.algo_param.ef_search": 100
                    }
                },
                "mappings": {
                    "properties": {
                        "procedure_name": {
                            "type": "text"
                        },
                        "sql_embedding": {
                            "type": "knn_vector",
                            "dimension": dimension  # Use the detected dimension
                        },
                        "table_name": {
                            "type": "keyword"
                        },
                        "view_name": {
                            "type": "keyword"
                        }
                    }
                }
            }
            
            client.indices.create(index=PROCEDURE_INDEX, body=mapping)
            print(f"Created index {PROCEDURE_INDEX} with dimension {dimension}")
        else:
            print(f"Index {PROCEDURE_INDEX} already exists and has correct dimension")
    except Exception as e:
        print(f"Error creating index: {str(e)}")
        raise

def parse_procedure_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Parse the SQL file to extract procedures, their names, and referenced tables/views
    """
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Pattern to match CREATE OR REPLACE FUNCTION blocks
    procedure_pattern = r"---\s+(\w+)(?:\s+---\s+(.*?))?(?:\s+CREATE\s+OR\s+REPLACE\s+FUNCTION\s+\w+\(.*?\)\s+.*?LANGUAGE\s+plpgsql;)"
    
    procedures = []
    for match in re.finditer(procedure_pattern, content, re.DOTALL):
        procedure_name = match.group(1)
        comments = match.group(2) if match.group(2) else ""
        
        # Extract the full procedure definition
        start_pos = match.span()[0]
        end_marker = "--- 调用示例"
        end_pos = content.find(end_marker, start_pos)
        if end_pos == -1:
            # If no end marker, go to the next procedure or end of file
            next_proc = content.find("--- ", start_pos + 1)
            end_pos = next_proc if next_proc != -1 else len(content)
        
        procedure_sql = content[start_pos:end_pos].strip()
        
        # Extract referenced tables and views from comments and SQL
        tables = []
        views = []
        
        # Look for tables in the SQL (common patterns)
        table_patterns = [
            r'FROM\s+(\w+)',
            r'JOIN\s+(\w+)',
            r'UPDATE\s+(\w+)',
            r'INSERT\s+INTO\s+(\w+)'
        ]
        
        for pattern in table_patterns:
            for table_match in re.finditer(pattern, procedure_sql, re.IGNORECASE):
                table_name = table_match.group(1)
                if "view" in comments.lower() and table_name in comments:
                    views.append(table_name)
                else:
                    tables.append(table_name)
        
        # Extract view names from comments
        view_pattern = r'视图\s+(\w+)'
        for view_match in re.finditer(view_pattern, comments):
            views.append(view_match.group(1))
            
        # Make lists unique
        tables = list(set(tables))
        views = list(set(views))
        
        procedures.append({
            "procedure_name": procedure_name,
            "sql_content": procedure_sql,
            "tables": tables,
            "views": views
        })
    
    return procedures

async def generate_embeddings(procedures: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Generate embeddings for each procedure's SQL content"""
    
    embedding_service = EmbeddingService()
    embedded_procedures = []
    
    for proc in procedures:
        # Generate embedding for the SQL content
        embedding = await embedding_service.get_embedding(proc["sql_content"])
        
        # Create document for OpenSearch
        doc = {
            "procedure_name": proc["procedure_name"],
            "sql_embedding": embedding,
            "table_name": proc["tables"],
            "view_name": proc["views"]
        }
        
        embedded_procedures.append(doc)
    
    return embedded_procedures

def index_procedures(client: OpenSearch, procedures: List[Dict[str, Any]]) -> None:
    """Index the procedures in OpenSearch"""
    
    # Prepare bulk indexing actions
    actions = [
        {
            "_index": PROCEDURE_INDEX,
            "_id": f"procedure_{i}",
            "_source": proc
        }
        for i, proc in enumerate(procedures)
    ]
    
    # Perform bulk indexing
    helpers.bulk(client, actions)
    print(f"Indexed {len(procedures)} procedures")

async def main_async():
    try:
        # Parse the SQL file
        script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sql_file_path = os.path.join(script_dir, "Mock", "procedure_mock.sql")
        
        procedures = parse_procedure_file(sql_file_path)
        print(f"Found {len(procedures)} procedures")
        
        # Generate embeddings
        embedded_procedures = await generate_embeddings(procedures)
        
        # Determine embedding dimension dynamically
        if embedded_procedures and "sql_embedding" in embedded_procedures[0]:
            embedding_dimension = len(embedded_procedures[0]["sql_embedding"])
            print(f"Detected embedding dimension: {embedding_dimension}")
        else:
            # Default to 1024 if we can't determine
            embedding_dimension = 1024
            print(f"Using default embedding dimension: {embedding_dimension}")
        
        # Connect to OpenSearch and create index
        client = connect_to_opensearch()
        await create_procedure_index(client, embedding_dimension)
        
        # Index the procedures
        index_procedures(client, embedded_procedures)
        
        print("Procedure embedding and indexing completed")
    except Exception as e:
        print(f"Error in main process: {str(e)}")
        print("If OpenSearch connection failed, verify that OpenSearch is running")
        print("and check connection settings at the top of this file.")

def main():
    # Run the async main function
    asyncio.run(main_async())

if __name__ == "__main__":
    main()