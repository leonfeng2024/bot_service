from opensearchpy import OpenSearch
from typing import Dict, Any, List, Tuple
import traceback
import os
import re
import asyncio
from opensearchpy import helpers
from config import (
    OPENSEARCH_HOST, 
    OPENSEARCH_PORT, 
    OPENSEARCH_USER, 
    OPENSEARCH_PASSWORD, 
    OPENSEARCH_USE_SSL,
    OPENSEARCH_VERIFY_CERTS
)

# Index name for procedures
PROCEDURE_INDEX = "procedure_index"

class OpenSearchConfig:
    """Configuration for OpenSearch connection."""
    HOST = OPENSEARCH_HOST
    PORT = OPENSEARCH_PORT
    URL = f"http://{HOST}:{PORT}"
    AUTH = (OPENSEARCH_USER, OPENSEARCH_PASSWORD) if OPENSEARCH_USER and OPENSEARCH_PASSWORD else None
    HEADERS = {"Content-Type": "application/json"}

class OpenSearchTools:
    def __init__(self, config: OpenSearchConfig = OpenSearchConfig):
        """
        Initialize OpenSearch client with configuration.
        
        Args:
            config: OpenSearch configuration class (default: OpenSearchConfig)
        """
        self.config = config
        self.client = OpenSearch(
            hosts=[{"host": config.HOST, "port": config.PORT}],
            http_auth=config.AUTH,
            headers=config.HEADERS,
            use_ssl=OPENSEARCH_USE_SSL,
            verify_certs=OPENSEARCH_VERIFY_CERTS,
            ssl_show_warn=False
        )

    def get_index_list(self) -> List[str]:
        """
        Get a list of all indices from OpenSearch, excluding system indices.
        
        Returns:
            List[str]: List of index names
        """
        try:
            # 获取所有索引
            response = self.client.indices.get_alias(index="*")
            
            # 过滤掉以 "." 开头的系统索引
            index_list = [
                index_name 
                for index_name in response.keys() 
                if not index_name.startswith(".")
            ]
            
            # 按字母顺序排序
            index_list.sort()
            
            print(f"Found {len(index_list)} indices")
            return index_list
                
        except Exception as e:
            print(f"Error getting index list: {str(e)}")
            print(traceback.format_exc())
            return []
            
    def get_index_infor(self) -> List[Dict[str, str]]:
        """
        Get detailed information about all indices in OpenSearch.
        
        Returns:
            List[Dict[str, str]]: List of index information including name, doc count, deleted docs, and size
        """
        try:
            # Get all indices stats
            stats = self.client.indices.stats(index="_all")
            
            # Get list of non-system indices
            indices = self.get_index_list()
            
            result = []
            for index_name in indices:
                if index_name in stats["indices"]:
                    index_stats = stats["indices"][index_name]["total"]
                    result.append({
                        "index_name": index_name,
                        "doc.count": str(index_stats["docs"]["count"]),
                        "docs.deleted": str(index_stats["docs"]["deleted"]),
                        "store.size": str(index_stats["store"]["size_in_bytes"] / 1024) + "kb"
                    })
            
            return result
                
        except Exception as e:
            print(f"Error getting index information: {str(e)}")
            print(traceback.format_exc())
            return []

    def delete_index_by_name(self, index_name: str) -> Dict[str, str]:
        """
        Delete an OpenSearch index by name.
        
        Args:
            index_name (str): Name of the index to delete
        
        Returns:
            Dict[str, str]: Operation result with status and message
        """
        try:
            # Check if index exists
            if not self.client.indices.exists(index=index_name):
                return {
                    "status": "failed",
                    "message": f"Index {index_name} does not exist"
                }
            
            # Delete the index
            response = self.client.indices.delete(
                index=index_name,
                headers={"Content-Type": "application/json"}
            )
            
            if response.get("acknowledged", False):
                return {
                    "status": "success",
                    "message": "Delete index success"
                }
            else:
                return {
                    "status": "failed",
                    "message": "Delete index failed"
                }
                
        except Exception as e:
            print(f"Error deleting index: {str(e)}")
            print(traceback.format_exc())
            return {
                "status": "failed",
                "message": f"Delete index failed: {str(e)}"
            }

    def bulk_insert_documents(self, index_name: str, document_name: str, documents: List[Dict[str, Any]]) -> Dict[str, str]:
        print(f"bulk_insert_documents: {index_name} {document_name} {documents}")
        try:
            if not self.client.indices.exists(index=index_name):
                return {
                    "status": "failed",
                    "message": f"Index {index_name} does not exist"
                }

            # 先删除现有文档
            self.delete_document_by_name(index_name, document_name)
            
            batch_size = 20
            # Insert documents in batches of 20
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]
                bulk_body = []
                for doc in batch:
                    bulk_body.append({"index": {"_index": index_name}})
                    bulk_body.append(doc)
                
                response = self.client.bulk(
                    body=bulk_body,
                    refresh=True
                )
                
                if response.get("errors", True):
                    return {
                        "status": "failed",
                        "message": "Error during bulk insert"
                    }
            
            return {
                "status": "success",
                "message": f"Successfully inserted {len(documents)} documents"
            }
            
        except Exception as e:
            print(f"Error in bulk insert: {str(e)}")
            print(traceback.format_exc())
            return {
                "status": "failed",
                "message": f"Bulk insert failed: {str(e)}"
            }

    def delete_document_by_name(self, index_name: str, document_name: str) -> Dict[str, str]:
        """
        Delete documents by document name from specified OpenSearch index.
        
        Args:
            index_name (str): Name of the index
            document_name (str): Name of the document to delete
        
        Returns:
            Dict[str, str]: Operation result with status and message
        """
        print(f" delete_document_by_name : {index_name} {document_name}")
        try:
            # 检查索引是否存在
            if not self.client.indices.exists(index=index_name):
                return {
                    "status": "failed",
                    "message": f"Index {index_name} does not exist"
                }
            
            # 构建删除查询 - 使用term查询精确匹配document_name字段
            query = {
                "query": {
                    "term": {
                        "document_name.keyword": document_name
                    }
                }
            }
            
            # 先尝试term查询，如果没有结果再尝试match查询
            response = self.client.delete_by_query(
                index=index_name,
                body=query,
                refresh=True
            )
            
            deleted_count = response.get("deleted", 0)
            if deleted_count > 0:
                return {
                    "status": "success",
                    "message": f"Successfully deleted {deleted_count} documents"
                }
            else:
                # 如果term查询没有结果，尝试使用match查询
                match_query = {
                    "query": {
                        "match": {
                            "document_name": document_name
                        }
                    }
                }
                
                match_response = self.client.delete_by_query(
                    index=index_name,
                    body=match_query,
                    refresh=True
                )
                
                match_deleted_count = match_response.get("deleted", 0)
                print(f"match_deleted_count : {match_deleted_count}")
                if match_deleted_count > 0:
                    return {
                        "status": "success",
                        "message": f"Successfully deleted {match_deleted_count} documents using match query"
                    }
                else:
                    return {
                        "status": "failed", 
                        "message": "No documents found to delete"
                    }
                
        except Exception as e:
            print(f"Error deleting document by name: {str(e)}")
            print(traceback.format_exc())
            return {
                "status": "failed",
                "message": f"Delete failed: {str(e)}"
            }

    def connect_to_opensearch(self) -> OpenSearch:
        """Create and return an OpenSearch client"""
        try:
            client = OpenSearch(
                hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
                http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
                use_ssl=OPENSEARCH_USE_SSL,
                verify_certs=OPENSEARCH_VERIFY_CERTS,
                ssl_show_warn=False,
                timeout=30
            )
            # Test connection
            client.info()
            print("Successfully connected to OpenSearch")
            self.client = client
            return client
        except Exception as e:
            print(f"Error connecting to OpenSearch: {str(e)}")
            print("Make sure OpenSearch is running and configuration is correct.")
            raise

    async def create_procedure_index(self, dimension: int, index_name: str = PROCEDURE_INDEX) -> None:
        """Create the procedure index with KNN mapping if it doesn't exist"""
        try:
            client = self.client or self.connect_to_opensearch()
            
            # Delete the index if it exists with wrong dimension or wrong field type
            if client.indices.exists(index=index_name):
                # Get the mapping
                mapping = client.indices.get_mapping(index=index_name)
                
                # Check vector dimension in the mapping
                if index_name in mapping:
                    props = mapping[index_name].get('mappings', {}).get('properties', {})
                    sql_embedding = props.get('sql_embedding', {})
                    
                    # Check if sql_embedding exists and is the correct type
                    if not sql_embedding or sql_embedding.get('type') != 'knn_vector':
                        print(f"Index {index_name} has incorrect field type for sql_embedding: {sql_embedding.get('type', 'not defined')}")
                        client.indices.delete(index=index_name)
                        print(f"Deleted index {index_name} to recreate with correct field type")
                    elif sql_embedding.get('type') == 'knn_vector' and sql_embedding.get('dimension') != dimension:
                        # If dimensions don't match, delete the index to recreate
                        print(f"Index {index_name} has dimension {sql_embedding.get('dimension')}, but need {dimension}")
                        client.indices.delete(index=index_name)
                        print(f"Deleted index {index_name} to recreate with correct dimension")
                    else:
                        print(f"Index {index_name} already has correct dimension {dimension}")
                        return
            
            if not client.indices.exists(index=index_name):
                mapping = {
                    "settings": {
                        "index": {
                            "knn": True,
                            "knn.algo_param.ef_search": 100
                        }
                    },
                    "mappings": {
                        "properties": {
                            "document_name": {
                                "type": "keyword"
                            },
                            "procedure_name": {
                                "type": "text"
                            },
                            "sql_content": {
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
                
                client.indices.create(index=index_name, body=mapping)
                print(f"Created index {index_name} with dimension {dimension}")
            else:
                print(f"Index {index_name} already exists and has correct dimension")
        except Exception as e:
            print(f"Error creating index: {str(e)}")
            raise

    def parse_procedure_file(self, file_path: str) -> List[Dict[str, Any]]:
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
            end_marker = "--- please call example"
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

    def index_procedures(self, procedures: List[Dict[str, Any]], index_name: str = PROCEDURE_INDEX) -> None:
        """Index the procedures in OpenSearch"""
        client = self.client or self.connect_to_opensearch()
        
        # Prepare bulk indexing actions
        actions = [
            {
                "_index": index_name,
                "_id": f"procedure_{i}",
                "_source": proc
            }
            for i, proc in enumerate(procedures)
        ]
        
        # Perform bulk indexing
        helpers.bulk(client, actions)
        print(f"Indexed {len(procedures)} procedures to {index_name}")

    async def process_sql_file(self, file_path: str, embedding_service, index_name: str = PROCEDURE_INDEX) -> Tuple[bool, str]:
        """
        Process SQL file and index procedures to OpenSearch
        
        Args:
            file_path: Path to SQL file
            embedding_service: EmbeddingService instance for generating embeddings
            index_name: Name of the index to use (default: PROCEDURE_INDEX)
            
        Returns:
            Tuple of (success, message)
        """
        try:
            # Connect to OpenSearch
            self.connect_to_opensearch()
            
            # Parse SQL file
            procedures = self.parse_procedure_file(file_path)
            print(f"Found {len(procedures)} procedures")
            
            if not procedures:
                return False, "No procedures found in file"
            
            # Get document name from file path (just the file name, not the full path)
            document_name = os.path.basename(file_path)
            
            # Generate embeddings for each procedure
            embedded_procedures = []
            for proc in procedures:
                # Generate embedding for the SQL content
                embedding = await embedding_service.get_embedding(proc["sql_content"])
                
                # Create document for OpenSearch
                doc = {
                    "document_name": document_name,
                    "procedure_name": proc["procedure_name"],
                    "sql_content": proc["sql_content"],
                    "sql_embedding": embedding,
                    "table_name": proc["tables"],
                    "view_name": proc["views"]
                }
                
                embedded_procedures.append(doc)
            
            # Determine embedding dimension dynamically
            if embedded_procedures and "sql_embedding" in embedded_procedures[0]:
                embedding_dimension = len(embedded_procedures[0]["sql_embedding"])
                print(f"Detected embedding dimension: {embedding_dimension}")
            else:
                # Default to 1024 if we can't determine
                embedding_dimension = 1024
                print(f"Using default embedding dimension: {embedding_dimension}")
            
            # Create procedure index with correct dimension
            await self.create_procedure_index(embedding_dimension, index_name)
            
            # 先删除已存在的相同document_name的文档
            delete_result = self.delete_document_by_name(index_name, document_name)
            if delete_result["status"] == "success" and "deleted" in delete_result["message"]:
                print(f"Deleted existing documents with name {document_name}")
            
            # Index the procedures
            self.index_procedures(embedded_procedures, index_name)
            
            return True, f"Successfully indexed {len(procedures)} procedures to OpenSearch index {index_name}"
            
        except Exception as e:
            error_msg = f"Error processing SQL file: {str(e)}"
            print(error_msg)
            return False, error_msg

    def delete_procedures_by_file(self, index_name: str) -> Dict[str, str]:
        """
        Delete all procedures in the specified index.
        This is used for SQL procedure files where we want to completely clear an index.
        
        Args:
            index_name (str): Name of the index to clear
        
        Returns:
            Dict[str, str]: Operation result with status and message
        """
        try:
            # 检查索引是否存在
            if not self.client.indices.exists(index=index_name):
                return {
                    "status": "failed",
                    "message": f"Index {index_name} does not exist"
                }
            
            # 构建删除所有文档的查询
            query = {
                "query": {
                    "match_all": {}
                }
            }
            
            # 执行删除操作
            response = self.client.delete_by_query(
                index=index_name,
                body=query,
                refresh=True
            )
            
            deleted_count = response.get("deleted", 0)
            if deleted_count > 0:
                return {
                    "status": "success",
                    "message": f"Successfully deleted {deleted_count} procedures from index {index_name}"
                }
            else:
                return {
                    "status": "success",
                    "message": "No procedures found to delete"
                }
                
        except Exception as e:
            print(f"Error deleting procedures from index: {str(e)}")
            print(traceback.format_exc())
            return {
                "status": "failed",
                "message": f"Delete failed: {str(e)}"
            }

    async def create_employees_index(self, client, dimension=1024):
        """Create the employees index with KNN mapping if it doesn't exist"""
        try:
            index_name = "employees"
            
            # Check if index exists and has wrong configuration
            if client.indices.exists(index=index_name):
                # Delete the index to recreate with correct structure
                client.indices.delete(index=index_name)
                print(f"Deleted index {index_name} to recreate with correct vector field")
            
            # Create index with proper mappings
            mapping = {
                "settings": {
                    "index": {
                        "knn": True,
                        "knn.algo_param.ef_search": 100
                    }
                },
                "mappings": {
                    "properties": {
                        "employee_id": {"type": "integer"},
                        "first_name": {"type": "text"},
                        "last_name": {"type": "text"},
                        "email": {"type": "keyword"},
                        "job_title": {"type": "text"},
                        "department_id": {"type": "integer"},
                        "salary": {"type": "float"},
                        # Add a vector embedding field
                        "sql_embedding": {
                            "type": "knn_vector",
                            "dimension": dimension
                        }
                    }
                }
            }
            
            client.indices.create(index=index_name, body=mapping)
            print(f"Created index {index_name} with dimension {dimension}")
            return True
            
        except Exception as e:
            print(f"Error creating index: {str(e)}")
            return False

    async def fix_index_field_types(self, index_name: str = PROCEDURE_INDEX, dimension: int = 1024) -> bool:
        """
        Fix an existing index with incorrect field types, specifically for sql_embedding field
        
        Args:
            index_name: Name of the index to fix
            dimension: Vector dimension for knn_vector field
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            client = self.client or self.connect_to_opensearch()
            
            # Check if index exists
            if not client.indices.exists(index=index_name):
                print(f"Index {index_name} does not exist")
                return False
            
            # Get the mapping
            mapping = client.indices.get_mapping(index=index_name)
            
            # Check if sql_embedding field exists and has incorrect type
            if index_name in mapping:
                props = mapping[index_name].get('mappings', {}).get('properties', {})
                sql_embedding = props.get('sql_embedding', {})
                
                if not sql_embedding:
                    print(f"Index {index_name} does not have sql_embedding field")
                    return False
                
                field_type = sql_embedding.get('type')
                if field_type == 'knn_vector':
                    print(f"Index {index_name} already has correct field type for sql_embedding: knn_vector")
                    return True
                
                print(f"Index {index_name} has incorrect field type for sql_embedding: {field_type}")
                
                # Need to recreate the index with correct field type
                # First, get all documents from the index
                query = {
                    "query": {
                        "match_all": {}
                    },
                    "size": 10000  # Adjust as needed
                }
                
                response = client.search(index=index_name, body=query)
                documents = []
                
                for hit in response['hits']['hits']:
                    documents.append(hit['_source'])
                
                print(f"Retrieved {len(documents)} documents from index {index_name}")
                
                # Delete the index
                client.indices.delete(index=index_name)
                print(f"Deleted index {index_name}")
                
                # Create the index with correct mapping
                await self.create_procedure_index(dimension, index_name)
                
                # Reindex the documents if any were found
                if documents:
                    # Need to generate embeddings for the documents
                    from service.embedding_service import EmbeddingService
                    embedding_service = EmbeddingService()
                    
                    # For each document, generate embedding if needed
                    for doc in documents:
                        if 'sql_content' in doc and 'sql_embedding' not in doc:
                            # Generate embedding
                            embedding = await embedding_service.get_embedding(doc['sql_content'])
                            doc['sql_embedding'] = embedding
                    
                    # Bulk index the documents
                    actions = []
                    for i, doc in enumerate(documents):
                        actions.append({
                            "_index": index_name,
                            "_source": doc
                        })
                    
                    if actions:
                        from opensearchpy import helpers
                        helpers.bulk(client, actions)
                        print(f"Reindexed {len(actions)} documents to index {index_name}")
                
                return True
            
            return False
            
        except Exception as e:
            print(f"Error fixing index field types: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return False

if __name__ == "__main__":
    # Initialize OpenSearch client with default config
    os_tools = OpenSearchTools()
    
    # Test get_index_list method
    print("Testing get_index_list method...")
    indices = os_tools.get_index_list()
    print("\nAvailable indices:")
    for index in indices:
        print(f"- {index}")
