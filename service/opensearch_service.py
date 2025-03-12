from typing import List, Dict, Any, Optional
from tools.opensearch_tools import OpenSearchTools, OpenSearchConfig
from service.embedding_service import EmbeddingService
from utils.singleton import singleton


@singleton
class OpenSearchService:
    def __init__(self):
        """Initialize OpenSearch service with tools and embedding service."""
        self.os_tools = OpenSearchTools()
        self.embedding_service = EmbeddingService()

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