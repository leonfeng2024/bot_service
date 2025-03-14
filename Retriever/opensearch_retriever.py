from typing import List, Dict, Any
import traceback
from opensearchpy import OpenSearch
from Retriever.base_retriever import BaseRetriever
from service.embedding_service import EmbeddingService

class OpenSearchRetriever(BaseRetriever):
    """OpenSearch检索器，用于从OpenSearch中检索数据"""
    
    def __init__(self):
        # OpenSearch connection settings
        self.opensearch_host = "localhost"
        self.opensearch_port = 9200
        self.opensearch_user = "admin"
        self.opensearch_password = "admin"
        self.use_ssl = False
        self.procedure_index = "procedure_index"
        self.vector_dim = 1024  # Update to match the actual embedding dimension
        
        # Initialize EmbeddingService for query vectorization
        self.embedding_service = EmbeddingService()
        
        # Initialize OpenSearch client
        self.client = self._connect_to_opensearch()
    
    def _connect_to_opensearch(self) -> OpenSearch:
        """Create and return an OpenSearch client"""
        try:
            client = OpenSearch(
                hosts=[{"host": self.opensearch_host, "port": self.opensearch_port}],
                http_auth=(self.opensearch_user, self.opensearch_password),
                use_ssl=self.use_ssl,
                verify_certs=False,
                ssl_show_warn=False,
                timeout=30
            )
            # Test connection
            client.info()
            print("OpenSearchRetriever successfully connected to OpenSearch")
            return client
        except Exception as e:
            print(f"OpenSearchRetriever error connecting to OpenSearch: {str(e)}")
            print("Make sure OpenSearch is running and configuration is correct.")
            return None
    
    async def _check_and_update_index(self, embedding_dimension: int) -> bool:
        """Check if the index exists and has the correct dimension, recreate if needed"""
        try:
            # Check if index exists
            if self.client.indices.exists(index=self.procedure_index):
                # Get the mapping
                mapping = self.client.indices.get_mapping(index=self.procedure_index)
                
                # Check vector dimension in the mapping
                if self.procedure_index in mapping:
                    props = mapping[self.procedure_index].get('mappings', {}).get('properties', {})
                    sql_embedding = props.get('sql_embedding', {})
                    
                    if sql_embedding and sql_embedding.get('type') == 'knn_vector':
                        current_dim = sql_embedding.get('dimension')
                        
                        # If dimensions match, no need to update
                        if current_dim == embedding_dimension:
                            print(f"Index {self.procedure_index} already has correct dimension {embedding_dimension}")
                            return True
                        
                        print(f"Index {self.procedure_index} has dimension {current_dim}, but need {embedding_dimension}")
                        
                        # Delete the index to recreate with correct dimension
                        self.client.indices.delete(index=self.procedure_index)
                        print(f"Deleted index {self.procedure_index} to recreate with correct dimension")
            
            # Create or recreate the index with the correct dimension
            index_config = {
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
                            "dimension": embedding_dimension
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
            
            self.client.indices.create(index=self.procedure_index, body=index_config)
            print(f"Created index {self.procedure_index} with dimension {embedding_dimension}")
            return True
            
        except Exception as e:
            print(f"Error checking/updating index: {str(e)}")
            return False
        
    async def retrieve(self, query: str) -> List[Dict[str, Any]]:
        try:
            if not self.client:
                self.client = self._connect_to_opensearch()
                if not self.client:
                    return [{"content": "Failed to connect to OpenSearch", "score": 0.0}]
            
            # Hardcoded search term for now: "employee_details"
            search_term = "employee_details"
            
            # Generate embedding for the search term
            search_embedding = await self.embedding_service.get_embedding(search_term)
            
            # Get and print the actual dimension of the embedding
            actual_dim = len(search_embedding)
            print(f"Actual embedding dimension: {actual_dim}")
            
            # Update the vector_dim to match the actual dimension
            self.vector_dim = actual_dim
            
            # Check and update index if needed
            await self._check_and_update_index(self.vector_dim)
            
            # Build KNN query
            knn_query = {
                "size": 5,
                "_source": ["procedure_name", "table_name", "view_name"],
                "query": {
                    "knn": {
                        "sql_embedding": {
                            "vector": search_embedding,
                            "k": 5
                        }
                    }
                },
                "post_filter": {
                    "bool": {
                        "should": [
                            {"term": {"table_name": search_term}},
                            {"term": {"view_name": search_term}}
                        ]
                    }
                }
            }
            
            # Execute search
            response = self.client.search(
                body=knn_query,
                index=self.procedure_index
            )
            
            # Process results
            results = []
            if response and "hits" in response and "hits" in response["hits"]:
                for hit in response["hits"]["hits"]:
                    source = hit["_source"]
                    procedure_name = source.get("procedure_name", "Unknown Procedure")
                    
                    # Extract table names and view names (they may be arrays)
                    table_names = source.get("table_name", [])
                    view_names = source.get("view_name", [])
                    
                    # Convert to string if they're arrays
                    tables = ", ".join(table_names) if isinstance(table_names, list) else table_names
                    views = ", ".join(view_names) if isinstance(view_names, list) else view_names
                    
                    # Create content string with procedure info
                    content = f"Procedure: {procedure_name}"
                    if tables:
                        content += f" | Tables: {tables}"
                    if views:
                        content += f" | Views: {views}"
                    
                    # Get score and add result
                    score = hit.get("_score", 0.0)
                    results.append({"content": content, "score": score})
            
            # Return default message if no results found
            if not results:
                return [{"content": f"No procedures found for '{search_term}'", "score": 0.0}]
                
            return results
            
        except Exception as e:
            print(f"OpenSearch retrieval error: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            return [{"content": f"Error querying OpenSearch: {str(e)}", "score": 0.0}] 