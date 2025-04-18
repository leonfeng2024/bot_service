from typing import List, Dict, Any
import traceback
import json
from opensearchpy import OpenSearch
from Retriever.base_retriever import BaseRetriever
from service.embedding_service import EmbeddingService
from service.llm_service import LLMService
from tools.redis_tools import RedisTools
from tools.token_counter import TokenCounter

class OpenSearchRetriever(BaseRetriever):
    """OpenSearch检索器，用于从OpenSearch中检索数据"""
    
    def __init__(self):
        # OpenSearch connection settings
        self.opensearch_host = "bibot_opensearch"
        self.opensearch_port = 9200
        self.opensearch_user = "admin"
        self.opensearch_password = "Leonfeng@2025"
        self.use_ssl = False
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
            # print("OpenSearchRetriever successfully connected to OpenSearch")
            return client
        except Exception as e:
            # print(f"OpenSearchRetriever error connecting to OpenSearch: {str(e)}")
            # print("Make sure OpenSearch is running and configuration is correct.")
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
                            # print(f"Index {self.procedure_index} already has correct dimension {embedding_dimension}")
                            return True
                        
                        # print(f"Index {self.procedure_index} has dimension {current_dim}, but need {embedding_dimension}")
                        
                        # Delete the index to recreate with correct dimension
                        self.client.indices.delete(index=self.procedure_index)
                        # print(f"Deleted index {self.procedure_index} to recreate with correct dimension")
            
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
            # print(f"Created index {self.procedure_index} with dimension {embedding_dimension}")
            return True
            
        except Exception as e:
            # print(f"Error checking/updating index: {str(e)}")
            return False
    
    async def _search_term(self, term: str) -> List[Dict[str, Any]]:
        """对单个字段名进行搜索"""
        try:
            # Generate embedding for the search term
            search_embedding = await self.embedding_service.get_embedding(term)
            
            # Get the actual dimension of the embedding
            actual_dim = len(search_embedding)
            # print(f"Search term: {term}, embedding dimension: {actual_dim}")
            
            # Update the vector_dim to match the actual dimension
            self.vector_dim = actual_dim
            
            # Check and update index if needed
            await self._check_and_update_index(self.vector_dim)
            
            # 使用两种查询方式
            # 1. 纯KNN查询 - 基于语义相似性
            knn_query = {
                "size": 5,
                "_source": ["procedure_name", "sql_content", "table_name", "view_name"],
                "query": {
                    "knn": {
                        "sql_embedding": {
                            "vector": search_embedding,
                            "k": 5
                        }
                    }
                }
            }

            # 2. 带过滤的KNN查询 - 结合精确匹配和语义相似性
            filtered_query = {
                "size": 5,
                "_source": ["procedure_name", "sql_content", "table_name", "view_name"],
                "query": {
                    "bool": {
                        "must": [
                            {
                                "knn": {
                                    "sql_embedding": {
                                        "vector": search_embedding,
                                        "k": 5
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
            
            # 首先尝试带过滤的查询
            # print(f"Executing filtered query for: {term}")
            response = self.client.search(
                body=filtered_query,
                index=self.procedure_index
            )
            
            # 如果没有结果，尝试纯KNN查询
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                # print(f"No results from filtered query, trying pure KNN query for: {term}")
                response = self.client.search(
                    body=knn_query,
                    index=self.procedure_index
                )
            
            # 打印原始结果以便调试
            hits = response.get("hits", {}).get("hits", [])
            # print(f"Found {len(hits)} results for term: {term}")
            # if hits:
            #     for hit in hits:
            #         print(f"Hit score: {hit.get('_score')}, procedure: {hit.get('_source', {}).get('procedure_name')}")
            #         # 添加SQL内容调试输出
            #         sql_content = hit.get('_source', {}).get('sql_content', 'No SQL content')
            #         print(f"SQL content for {hit.get('_source', {}).get('procedure_name')}: {sql_content[:100]}...")
            
            # Process results
            results = []
            if response and "hits" in response and "hits" in response["hits"]:
                for hit in response["hits"]["hits"]:
                    source = hit["_source"]
                    procedure_name = source.get("procedure_name", "Unknown")
                    sql_content = source.get("sql_content", "")
                    table_name = source.get("table_name", "")
                    view_name = source.get("view_name", "")
                    
                    # 基本内容
                    base_content = f"Procedure '{procedure_name}':\n{sql_content}"
                    
                    # 添加与搜索词相关的说明
                    enhanced_content = f"{base_content}\n\n与{term}相关的procedure是：{sql_content}"
                    
                    # 添加表和视图信息（如果有）
                    if table_name:
                        enhanced_content += f"\n相关表: {table_name}"
                    if view_name:
                        enhanced_content += f"\n相关视图: {view_name}"
                    
                    # 返回增强后的内容
                    results.append({"content": enhanced_content, "score": hit.get("_score", 0.89)})
            
            # 如果没有找到结果，添加一个友好的提示
            if not results:
                results.append({"content": f"未找到与'{term}'相关的存储过程。", "score": 0.5})
            
            return results
            
        except Exception as e:
            # print(f"Error searching for term '{term}': {str(e)}")
            # print(f"Detailed error: {traceback.format_exc()}")
            return [{"content": f"Error searching for '{term}': {str(e)}", "score": 0.89}]
        
    async def retrieve(self, query: str, uuid: str = None) -> List[Dict[str, Any]]:
        """从OpenSearch检索相关信息"""
        try:
            # 记录开始时的token使用情况
            start_usage = self.llm_service.get_token_usage()
            
            # 获取查询的嵌入向量
            query_embedding = await self.embedding_service.get_embedding(query)
            
            # 使用嵌入向量进行搜索
            results = await self._search_term(query)
            
            # 记录结束时的token使用情况
            end_usage = self.llm_service.get_token_usage()
            
            # 计算本次调用消耗的token
            input_tokens = end_usage["input_tokens"] - start_usage["input_tokens"]
            output_tokens = end_usage["output_tokens"] - start_usage["output_tokens"]
            
            # 打印token使用情况
            print(f"[OpenSearch Retriever] Total token usage - Input: {input_tokens} tokens, Output: {output_tokens} tokens")
            
            # 添加token使用信息到结果中
            for result in results:
                result["token_usage"] = {
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens
                }
            
            # 如果提供了uuid，将结果存储到Redis
            if uuid:
                try:
                    key = f"{uuid}:opensearch"
                    self.redis_tools.set(key, results)
                except Exception as redis_error:
                    print(f"Error storing OpenSearch results in Redis: {str(redis_error)}")
            
            return results
            
        except Exception as e:
            import traceback
            print(f"OpenSearch retriever error: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            
            # 构建错误响应
            error_result = [{
                "content": f"Error querying OpenSearch: {str(e)}", 
                "score": 0.0, 
                "source": "opensearch",
                "token_usage": {
                    "input_tokens": 0,
                    "output_tokens": 0
                }
            }]
            
            # 如果提供了uuid，尝试将错误结果存储到Redis
            if uuid:
                try:
                    key = f"{uuid}:opensearch"
                    self.redis_tools.set(key, [])
                except Exception:
                    pass
                    
            return error_result
            
        except Exception as e:
            # print(f"OpenSearch retrieval error: {str(e)}")
            # print(f"Detailed error: {traceback.format_exc()}")
            
            # 如果提供了UUID，将空结果存储到Redis
            if uuid:
                try:
                    # 获取现有的缓存数据
                    cached_data = self.redis_tools.get(uuid) or {}
                    # 设置空字符串
                    cached_data["opensearch"] = ""
                    # 更新Redis缓存
                    self.redis_tools.set(uuid, cached_data)
                except Exception:
                    pass
                
            return [{"content": f"Error querying OpenSearch: {str(e)}", "score": 0}]