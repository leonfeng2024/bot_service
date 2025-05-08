import logging
import os
import json
import asyncio
from typing import List, Dict, Any, Optional
import requests
from opensearchpy import OpenSearch
from config import (
    OPENSEARCH_HOST, OPENSEARCH_PORT, OPENSEARCH_USER, OPENSEARCH_PASSWORD,
    OPENSEARCH_USE_SSL, OPENSEARCH_VERIFY_CERTS
)
from tools.opensearch_tools import OpenSearchTools, OpenSearchConfig
from service.embedding_service import EmbeddingService
from utils.singleton import singleton

logger = logging.getLogger(__name__)

@singleton
class OpenSearchService:
    def __init__(self):
        """Initialize OpenSearch service with tools and embedding service."""
        self.os_tools = OpenSearchTools()
        self.embedding_service = EmbeddingService()
        self.client = None
        self._connect()

    def _connect(self):
        """Connect to OpenSearch server"""
        try:
            # Get SSL settings from configuration
            use_ssl = OPENSEARCH_USE_SSL
            verify_certs = OPENSEARCH_VERIFY_CERTS
            
            logger.info(f"Connecting to OpenSearch at {OPENSEARCH_HOST}:{OPENSEARCH_PORT} (SSL: {use_ssl}, Verify: {verify_certs})")
            
            # Create OpenSearch client
            self.client = OpenSearch(
                hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
                http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
                use_ssl=use_ssl,
                verify_certs=verify_certs,
                ssl_show_warn=False
            )
            
            # Test connection
            try:
                info = self.client.info()
                logger.info(f"Successfully connected to OpenSearch {info.get('version', {}).get('number', 'unknown')} at {OPENSEARCH_HOST}:{OPENSEARCH_PORT}")
                return
            except Exception as e:
                logger.warning(f"Failed to connect with current settings: {str(e)}")
                
                # If SSL fails, try without SSL
                if use_ssl:
                    logger.info("Attempting connection without SSL...")
                    self.client = OpenSearch(
                        hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
                        http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
                        use_ssl=False
                    )
                    try:
                        self.client.info()
                        logger.info(f"Successfully connected to OpenSearch at {OPENSEARCH_HOST}:{OPENSEARCH_PORT} without SSL")
                        return
                    except Exception as no_ssl_error:
                        logger.warning(f"Failed to connect without SSL: {str(no_ssl_error)}")
                
                # Try as a last resort, possibly local dev environment without auth
                try:
                    logger.info("Attempting connection without authentication...")
                    self.client = OpenSearch(
                        hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
                        use_ssl=False
                    )
                    self.client.info()
                    logger.info(f"Successfully connected to OpenSearch at {OPENSEARCH_HOST}:{OPENSEARCH_PORT} without authentication")
                    return
                except Exception as no_auth_error:
                    logger.warning(f"Failed to connect without authentication: {str(no_auth_error)}")
                    raise
                
        except Exception as e:
            import traceback
            logger.error(f"Failed to connect to OpenSearch: {str(e)}")
            logger.error(traceback.format_exc())
            # Don't raise exception, allow system to continue running

    async def get_indices(self) -> List[Dict[str, Any]]:
        """
        Get information about all indices
        
        Returns:
            List of index information
        """
        try:
            # Ensure client is connected
            if not self.client:
                self._connect()
                if not self.client:
                    raise Exception("Unable to connect to OpenSearch")
            
            # Get all index information
            indices_info = []
            cats = self.client.cat.indices(format="json")
            cluster_health = self.client.cluster.health()
            
            for index in cats:
                index_name = index.get('index')
                # Get index health status
                health = "yellow"  # Default value
                if index_name in cluster_health.get('indices', {}):
                    health = cluster_health['indices'][index_name]['status']
                elif 'status' in cluster_health:
                    health = cluster_health['status']
                
                indices_info.append({
                    "index": index_name,
                    "doc_count": int(index.get('docs.count', 0)),
                    "status": index.get('status', 'unknown'),
                    "health": health
                })
            
            return indices_info
            
        except Exception as e:
            logger.error(f"Error getting indices: {str(e)}")
            raise

    async def create_index(self, index_name: str) -> Dict[str, Any]:
        """
        Create a new index
        
        Args:
            index_name: Name of the index
            
        Returns:
            Creation result
        """
        try:
            # Ensure client is connected
            if not self.client:
                self._connect()
                if not self.client:
                    raise Exception("Unable to connect to OpenSearch")
            
            # Check if index already exists
            if self.client.indices.exists(index=index_name):
                return {
                    "success": False,
                    "message": f"Index '{index_name}' already exists"
                }
            
            # Create index configuration
            index_config = {
                "settings": {
                    "index": {
                        "number_of_shards": 1,
                        "number_of_replicas": 1
                    }
                },
                "mappings": {
                    "properties": {
                        "title": {"type": "text"},
                        "content": {"type": "text"},
                        "metadata": {"type": "object"}
                    }
                }
            }
            
            # Create index
            response = self.client.indices.create(
                index=index_name,
                body=index_config
            )
            
            if response.get('acknowledged', False):
                return {
                    "success": True,
                    "message": f"Index '{index_name}' created successfully"
                }
            else:
                return {
                    "success": False,
                    "message": f"Failed to create index '{index_name}'"
                }
            
        except Exception as e:
            logger.error(f"Error creating index {index_name}: {str(e)}")
            return {
                "success": False,
                "message": f"Error creating index: {str(e)}"
            }

    async def delete_index(self, index_name: str) -> Dict[str, Any]:
        """
        Delete an index
        
        Args:
            index_name: Name of the index
            
        Returns:
            Deletion result
        """
        try:
            # Ensure client is connected
            if not self.client:
                self._connect()
                if not self.client:
                    raise Exception("Unable to connect to OpenSearch")
            
            # Check if index exists
            if not self.client.indices.exists(index=index_name):
                return {
                    "success": False,
                    "message": f"Index '{index_name}' does not exist"
                }
            
            # Delete index
            response = self.client.indices.delete(index=index_name)
            
            if response.get('acknowledged', False):
                return {
                    "success": True,
                    "message": f"Index '{index_name}' deleted successfully"
                }
            else:
                return {
                    "success": False,
                    "message": f"Failed to delete index '{index_name}'"
                }
            
        except Exception as e:
            logger.error(f"Error deleting index {index_name}: {str(e)}")
            return {
                "success": False,
                "message": f"Error deleting index: {str(e)}"
            }

    async def search(self, index_name: str, query_string: str) -> Dict[str, Any]:
        """
        Execute search in specified index
        
        Args:
            index_name: Name of the index
            query_string: Query string
            
        Returns:
            Search results
        """
        try:
            # Ensure client is connected
            if not self.client:
                self._connect()
                if not self.client:
                    raise Exception("Unable to connect to OpenSearch")
            
            # Build query
            query = {
                "query": {
                    "query_string": {
                        "query": query_string
                    }
                }
            }
            
            # Execute search
            response = self.client.search(
                body=query,
                index=index_name
            )
            
            # Process search results
            hits = []
            for hit in response['hits']['hits']:
                hits.append({
                    "id": hit['_id'],
                    "index": hit['_index'],
                    "score": hit['_score'],
                    "source": hit['_source']
                })
            
            return {"hits": hits}
            
        except Exception as e:
            logger.error(f"Error searching index {index_name}: {str(e)}")
            raise

    async def upload_document(self, index_name: str, file_content: bytes, file_name: str) -> Dict[str, Any]:
        """
        Upload document to index
        
        Args:
            index_name: Name of the index
            file_content: File content
            file_name: File name
            
        Returns:
            Upload result
        """
        try:
            # Ensure client is connected
            if not self.client:
                self._connect()
                if not self.client:
                    raise Exception("Unable to connect to OpenSearch")
            
            # Check if index exists, create if not
            if not self.client.indices.exists(index=index_name):
                create_result = await self.create_index(index_name)
                if not create_result['success']:
                    return create_result
            
            # Parse file content
            file_ext = os.path.splitext(file_name)[1].lower()
            
            # Handle different file types
            content = ""
            if file_ext == '.txt':
                content = file_content.decode('utf-8')
            elif file_ext in ['.doc', '.docx']:
                # Need additional library to handle Word documents
                content = self._extract_text_from_word(file_content)
            elif file_ext in ['.xls', '.xlsx']:
                # Need additional library to handle Excel documents
                content = self._extract_text_from_excel(file_content)
            else:
                return {
                    "success": False,
                    "message": f"Unsupported file type: {file_ext}"
                }
            
            # Create document
            document = {
                "title": file_name,
                "content": content,
                "metadata": {
                    "filename": file_name,
                    "filetype": file_ext[1:],
                    "upload_time": self._get_current_timestamp()
                }
            }
            
            # Index document
            response = self.client.index(
                index=index_name,
                body=document,
                refresh=True
            )
            
            if response.get('result') == 'created':
                return {
                    "success": True,
                    "message": f"Document '{file_name}' uploaded successfully to index '{index_name}'"
                }
            else:
                return {
                    "success": False,
                    "message": f"Failed to upload document to index '{index_name}'"
                }
            
        except Exception as e:
            logger.error(f"Error uploading document to index {index_name}: {str(e)}")
            return {
                "success": False,
                "message": f"Error uploading document: {str(e)}"
            }

    def _extract_text_from_word(self, file_content: bytes) -> str:
        """
        Extract text from Word document
        
        Args:
            file_content: Word document content
            
        Returns:
            Extracted text
        """
        try:
            # Save file content to temporary file
            temp_file = "temp_word.docx"
            with open(temp_file, "wb") as f:
                f.write(file_content)
            
            # Use textract to extract text
            import textract
            text = textract.process(temp_file).decode('utf-8')
            
            # Delete temporary file
            os.remove(temp_file)
            
            return text
        except Exception as e:
            logger.error(f"Error extracting text from Word document: {str(e)}")
            return ""

    def _extract_text_from_excel(self, file_content: bytes) -> str:
        """
        Extract text from Excel document
        
        Args:
            file_content: Excel document content
            
        Returns:
            Extracted text
        """
        try:
            # Save file content to temporary file
            temp_file = "temp_excel.xlsx"
            with open(temp_file, "wb") as f:
                f.write(file_content)
            
            # Use pandas to read Excel
            import pandas as pd
            dfs = pd.read_excel(temp_file, sheet_name=None)
            
            # Delete temporary file
            os.remove(temp_file)
            
            # Convert all worksheets to text
            texts = []
            for sheet_name, df in dfs.items():
                texts.append(f"Sheet: {sheet_name}")
                texts.append(df.to_string())
            
            return "\n\n".join(texts)
        except Exception as e:
            logger.error(f"Error extracting text from Excel document: {str(e)}")
            return ""

    def _get_current_timestamp(self) -> str:
        """
        Get current timestamp
        
        Returns:
            ISO format timestamp
        """
        from datetime import datetime
        return datetime.now().isoformat()

    async def text_search(
        self,
        query: str,
        index_name: str,
        fields: List[str] = ["content"],
        size: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Perform full-text search on specified index and fields.

        Args:
            query (str): User's search query
            index_name (str): Name of the index to search in
            fields (List[str]): List of fields to search in (default: ["content"])
            size (int): Number of results to return (default: 10)

        Returns:
            List[Dict[str, Any]]: List of search results with scores
        """
        try:
            # Check if index exists
            if not self.os_tools.client.indices.exists(index=index_name):
                return []

            # Construct multi-match query
            search_body = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": fields,
                        "type": "best_fields",
                        "operator": "or"
                    }
                },
                "size": size
            }

            # Execute search
            response = self.os_tools.client.search(
                index=index_name,
                body=search_body
            )

            # Process results
            results = []
            for hit in response["hits"]["hits"]:
                result = {
                    "content": hit["_source"],
                    "score": hit["_score"]
                }
                results.append(result)

            return results

        except Exception as e:
            import traceback
            print(f"Error in text search: {str(e)}")
            print(traceback.format_exc())
            return []

    async def knn_search(
        self,
        query: str,
        index_name: str,
        embedding_field: str = "embedding",
        size: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Perform KNN search using query embedding.

        Args:
            query (str): User's search query
            index_name (str): Name of the index to search in
            embedding_field (str): Name of the field containing embeddings (default: "embedding")
            size (int): Number of results to return (default: 10)

        Returns:
            List[Dict[str, Any]]: List of search results with scores
        """
        try:
            # Check if index exists
            if not self.os_tools.client.indices.exists(index=index_name):
                return []

            # Get query embedding
            query_embedding = await self.embedding_service.get_embedding(query)

            # Construct KNN query
            search_body = {
                "knn": {
                    "field": embedding_field,
                    "query_vector": query_embedding,
                    "k": size,
                    "num_candidates": size * 2
                },
                "size": size
            }

            # Execute search
            response = self.os_tools.client.search(
                index=index_name,
                body=search_body
            )

            # Process results
            results = []
            for hit in response["hits"]["hits"]:
                result = {
                    "content": hit["_source"],
                    "score": hit["_score"]
                }
                results.append(result)

            return results

        except Exception as e:
            import traceback
            print(f"Error in KNN search: {str(e)}")
            print(traceback.format_exc())
            return [] 