from typing import List, Dict, Any
import traceback
import json
from opensearchpy import OpenSearch
from Retriever.base_retriever import BaseRetriever
from service.embedding_service import EmbeddingService
from service.llm_service import LLMService
from tools.redis_tools import RedisTools
from tools.token_counter import TokenCounter
import config

class OpenSearchRetriever(BaseRetriever):
    """OpenSearch retriever for retrieving data from OpenSearch"""
    
    def __init__(self):
        # OpenSearch connection settings - read from config
        self.opensearch_host = config.OPENSEARCH_HOST
        self.opensearch_port = config.OPENSEARCH_PORT
        self.opensearch_user = config.OPENSEARCH_USER
        self.opensearch_password = config.OPENSEARCH_PASSWORD
        self.use_ssl = config.OPENSEARCH_USE_SSL
        self.procedure_index = "procedure_index"
        self.vector_dim = 1024  # Update to match the actual embedding dimension
        
        # Initialize EmbeddingService for query vectorization
        self.embedding_service = EmbeddingService()
        
        # Initialize LLMService for column identification
        self.llm_service = LLMService()
        
        # Initialize Redis tools for caching
        self.redis_tools = RedisTools()
        
        # Initialize token counter
        self.token_counter = TokenCounter()
        
        # Initialize OpenSearch client
        self.client = self._connect_to_opensearch()
    
    def _connect_to_opensearch(self) -> OpenSearch:
        """Create and return an OpenSearch client"""
        try:
            print(f"Connecting to OpenSearch at {self.opensearch_host}:{self.opensearch_port}")
            client = OpenSearch(
                hosts=[{"host": self.opensearch_host, "port": self.opensearch_port}],
                http_auth=(self.opensearch_user, self.opensearch_password),
                use_ssl=self.use_ssl,
                verify_certs=False,
                ssl_show_warn=False,
                timeout=30
            )
            # Test connection
            info = client.info()
            print(f"OpenSearchRetriever successfully connected to OpenSearch {info.get('version', {}).get('number', 'unknown')}")
            return client
        except Exception as e:
            print(f"Error connecting to OpenSearch at {self.opensearch_host}:{self.opensearch_port}: {str(e)}")
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
                            return True
            
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
                        "sql_content": {
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
            return True
            
        except Exception as e:
            return False
    
    async def _search_term(self, term: str) -> List[Dict[str, Any]]:
        """Search for a single term"""
        try:
            # Generate embedding for the search term
            search_embedding = await self.embedding_service.get_embedding(term)
            
            # Get the actual dimension of the embedding
            actual_dim = len(search_embedding)
            
            # Update the vector_dim to match the actual dimension
            self.vector_dim = actual_dim
            
            # Check and update index if needed
            await self._check_and_update_index(self.vector_dim)
            
            # Use two query approaches
            # 1. Pure KNN query - based on semantic similarity
            knn_query = {
                "size": 10,
                "_source": ["procedure_name", "sql_content", "table_name", "view_name"],
                "query": {
                    "knn": {
                        "sql_embedding": {
                            "vector": search_embedding,
                            "k": 10
                        }
                    }
                }
            }

            # 2. Filtered KNN query - combining exact match and semantic similarity
            filtered_query = {
                "size": 10,
                "_source": ["procedure_name", "sql_content", "table_name", "view_name"],
                "query": {
                    "bool": {
                        "must": [
                            {
                                "knn": {
                                    "sql_embedding": {
                                        "vector": search_embedding,
                                        "k": 10
                                    }
                                }
                            }
                        ],
                        "should": [
                            {"wildcard": {"table_name": f"*{term}*"}},
                            {"wildcard": {"view_name": f"*{term}*"}},
                            {"match": {"sql_content": term}},
                            {"match": {"procedure_name": term}}
                        ],
                        "minimum_should_match": 1
                    }
                }
            }
            
            # First try filtered query
            response = self.client.search(
                body=filtered_query,
                index=self.procedure_index
            )
            
            # If no results, try pure KNN query
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                response = self.client.search(
                    body=knn_query,
                    index=self.procedure_index
                )
            
            # Process results
            results = []
            if response and "hits" in response and "hits" in response["hits"]:
                for hit in response["hits"]["hits"]:
                    source = hit["_source"]
                    procedure_name = source.get("procedure_name", "Unknown")
                    sql_content = source.get("sql_content", "")
                    table_name = source.get("table_name", "")
                    view_name = source.get("view_name", "")
                    
                    # Basic content
                    base_content = f"Procedure '{procedure_name}':\n{sql_content}"
                    
                    # Add explanation related to search term
                    enhanced_content = f"{base_content}\n\nProcedure related to {term} is: {sql_content}"
                    
                    # Add table and view information (if available)
                    if table_name:
                        enhanced_content += f"\nRelated table: {table_name}"
                    if view_name:
                        enhanced_content += f"\nRelated view: {view_name}"
                    
                    # Return enhanced content
                    results.append({"content": enhanced_content, "score": hit.get("_score", 0.89)})
            
            # If no results found, add a friendly message
            if not results:
                results.append({"content": f"No stored procedures found related to '{term}'.", "score": 0.5})
            
            return results
            
        except Exception as e:
            return [{"content": f"Error searching for '{term}': {str(e)}", "score": 0.89}]
        
    async def _filter_results_with_llm(self, query: str, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Use LLM to filter search results, removing content unrelated to user query"""
        print(f"_filter_results_with_llm")
        try:
            if not results:
                return results
                
            # Create text containing all results
            results_text = ""
            for i, result in enumerate(results):
                results_text += f"Result {i+1}:\n{result.get('content', '')}\n\n"
                
            # Create prompt text
            prompt = f"""
            You are a professional database knowledge reviewer. Please review the following results retrieved from OpenSearch and determine which ones are relevant to the user's query and which ones are not.
            
            User query: {query}
            
            Retrieved results:
            {results_text}
            
            Please perform the following tasks:
            1. Analyze the relevance of each retrieved result to the user's query
            2. Remove content completely unrelated to the user's query
            3. Keep all relevant or potentially relevant content
            4. Remove duplicate content, if there are identical search results, keep only one
            5. Return the filtered results in JSON format as follows:
            [
                {{"content": "relevant content 1", "score": original_score, "relevance": "explain why relevant"}},
                {{"content": "relevant content 2", "score": original_score, "relevance": "explain why relevant"}}
            ]
            
            Return only the JSON format results, no other text explanation. If all results are irrelevant, return an empty array []."""
            
            # Call LLM service for filtering
            print(f"Prompt: {prompt}")
            # Get LLM instance
            llm = self.llm_service.get_llm()
            # Generate text using LLM
            llm_response = await llm.generate(prompt)
            print(f"LLM response: {llm_response}")
            
            # Try to parse JSON response
            try:
                # Extract JSON part (if LLM returned additional text)
                json_str = llm_response.strip()
                if json_str.startswith("```json"):
                    json_str = json_str.split("```json")[1]
                if json_str.endswith("```"):
                    json_str = json_str.split("```")[0]
                    
                filtered_results = json.loads(json_str.strip())
                
                # Ensure results format is correct
                if isinstance(filtered_results, list):
                    # Remove possible extra fields, maintain original result format
                    standardized_results = []
                    for item in filtered_results:
                        if isinstance(item, dict) and "content" in item:
                            result = {
                                "content": item["content"],
                                "score": item.get("score", 0.5)
                            }
                            # Keep token_usage if exists
                            if "token_usage" in results[0]:
                                result["token_usage"] = results[0]["token_usage"]
                            standardized_results.append(result)
                    
                    return standardized_results if standardized_results else results
                
            except Exception as json_error:
                print(f"Error parsing LLM response as JSON: {str(json_error)}")
                print(f"Raw LLM response: {llm_response}")
                
            # If parsing fails, return original results
            return results
            
        except Exception as e:
            print(f"Error filtering results with LLM: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            # Return original results on error
            return results
    
    async def retrieve(self, query: str, uuid: str = None) -> List[Dict[str, Any]]:
        """
        Retrieve data from OpenSearch related to the query
        
        Args:
            query: User query
            uuid: Session ID (optional)
            
        Returns:
            List of retrieval results
        """
        try:
            # Check client connection
            if not self.client:
                print("OpenSearch client is not connected, attempting to reconnect...")
                self.client = self._connect_to_opensearch()
                if not self.client:
                    error_msg = "Unable to connect to OpenSearch server, please check configuration"
                    error_result = [{"content": error_msg, "score": 0.0, "source": "opensearch"}]
                    
                    # If UUID provided, cache result
                    if uuid:
                        try:
                            key = f"{uuid}:opensearch"
                            self.redis_tools.set(key, error_result)
                            # Also update main cache
                            cached_data = self.redis_tools.get(uuid) or {}
                            cached_data["opensearch"] = error_result
                            self.redis_tools.set(uuid, cached_data)
                            print(f"Cached OpenSearch error result for {uuid}")
                        except Exception as cache_error:
                            print(f"Error caching OpenSearch error result: {str(cache_error)}")
                    
                    return error_result
            
            # Record token usage at start
            start_usage = self.llm_service.get_token_usage()
            
            # Build and print prompt
            prompt = f"OpenSearch retrieval query:\n{query}"
            print(prompt)
            
            # Call LLM service to get query intent
            intent_analysis = {}
            async for result in self.llm_service.identify_column(query):
                if isinstance(result, dict):
                    if "step" not in result:
                        intent_analysis = result
            print(f"Identified terms: {intent_analysis}")
            
            results = []
            
            # If intent was identified, search for each key term
            if intent_analysis:
                for key, term in intent_analysis.items():
                    print(f"Searching for term: {term}")
                    term_results = await self._search_term(term)
                    results.extend(term_results)
            
            # If no intent identified or no results, use original query directly
            if not intent_analysis or not results:
                print(f"No intent identified or no results, using original query: {query}")
                direct_results = await self._search_term(query)
                results.extend(direct_results)
            
            # If more than 3 results, use LLM to filter
            if len(results) > 3:
                results = await self._filter_results_with_llm(query, results)
            
            # Record token usage at end
            end_usage = self.llm_service.get_token_usage()
            
            # Calculate token usage
            input_tokens = end_usage["input_tokens"] - start_usage["input_tokens"]
            output_tokens = end_usage["output_tokens"] - start_usage["output_tokens"]
            
            # Print token usage
            print(f"[OpenSearch Retriever] Total token usage - Input: {input_tokens} tokens, Output: {output_tokens} tokens")
            
            # Update token counter
            self.token_counter.total_input_tokens += input_tokens
            self.token_counter.total_output_tokens += output_tokens
            
            # Record call history
            call_record = {
                "source": "opensearch-retriever",
                "input_tokens": input_tokens,
                "output_tokens": output_tokens
            }
            self.token_counter.calls_history.append(call_record)
            
            # Ensure results are not empty
            if not results:
                results = [{"content": "No relevant stored procedures or SQL code found", "score": 0.0, "source": "opensearch"}]
            
            # Add metadata to results
            for result in results:
                result["source"] = "opensearch"
                result["token_usage"] = {
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens
                }
            
            # If UUID provided, cache results
            if uuid:
                try:
                    # Use dedicated key
                    key = f"{uuid}:opensearch"
                    self.redis_tools.set(key, results)
                    
                    # Also update main cache
                    cached_data = self.redis_tools.get(uuid) or {}
                    cached_data["opensearch"] = results
                    self.redis_tools.set(uuid, cached_data)
                    print(f"Cached OpenSearch results for {uuid}")
                except Exception as cache_error:
                    print(f"Error caching OpenSearch results: {str(cache_error)}")
            
            return results
            
        except Exception as e:
            error_msg = f"OpenSearch retrieval error: {str(e)}"
            print(error_msg)
            print(traceback.format_exc())
            
            error_result = [{"content": error_msg, "score": 0.0, "source": "opensearch"}]
            
            # If UUID provided, cache error result
            if uuid:
                try:
                    key = f"{uuid}:opensearch"
                    self.redis_tools.set(key, error_result)
                    # Also update main cache
                    cached_data = self.redis_tools.get(uuid) or {}
                    cached_data["opensearch"] = error_result
                    self.redis_tools.set(uuid, cached_data)
                except Exception:
                    pass  # Ignore cache errors
            
            return error_result