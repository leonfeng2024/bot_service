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
    """OpenSearch检索器，用于从OpenSearch中检索数据"""
    
    def __init__(self):
        # OpenSearch connection settings - 从配置中读取
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
            print("Trying localhost as fallback...")
            
            try:
                # 尝试使用localhost
                client = OpenSearch(
                    hosts=[{"host": "localhost", "port": self.opensearch_port}],
                    http_auth=(self.opensearch_user, self.opensearch_password),
                    use_ssl=self.use_ssl,
                    verify_certs=False,
                    ssl_show_warn=False,
                    timeout=30
                )
                # Test connection
                info = client.info()
                print(f"OpenSearchRetriever successfully connected to OpenSearch at localhost:{self.opensearch_port}")
                return client
            except Exception as local_e:
                print(f"Error connecting to OpenSearch at localhost:{self.opensearch_port}: {str(local_e)}")
                print("Trying 127.0.0.1 as fallback...")
                
                try:
                    # 尝试使用IP地址
                    client = OpenSearch(
                        hosts=[{"host": "127.0.0.1", "port": self.opensearch_port}],
                        http_auth=(self.opensearch_user, self.opensearch_password),
                        use_ssl=self.use_ssl,
                        verify_certs=False,
                        ssl_show_warn=False,
                        timeout=30
                    )
                    # Test connection
                    info = client.info()
                    print(f"OpenSearchRetriever successfully connected to OpenSearch at 127.0.0.1:{self.opensearch_port}")
                    return client
                except Exception as ip_e:
                    print(f"Error connecting to OpenSearch at 127.0.0.1:{self.opensearch_port}: {str(ip_e)}")
                    print("All connection attempts failed.")
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
        
    async def _filter_results_with_llm(self, query: str, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """使用大语言模型过滤搜索结果，删除与用户查询无关的内容"""
        print(f"_filter_results_with_llm")
        try:
            if not results:
                return results
                
            # 创建包含所有结果的文本
            results_text = ""
            for i, result in enumerate(results):
                results_text += f"Result {i+1}:\n{result.get('content', '')}\n\n"
                
            # 创建提示文本
            prompt = f"""
            你是一个专业的数据库知识审查专家。请审查以下从OpenSearch检索到的结果，判断哪些与用户查询相关，哪些无关。
            
            用户查询: {query}
            
            检索结果:
            {results_text}
            
            请执行以下任务:
            1. 分析每个检索结果与用户查询的相关性
            2. 删除与用户查询完全无关的内容
            3. 保留所有相关或可能相关的内容
            4. 去掉重复内容，如果有相同的检索结果，只保留一个
            5. 以JSON格式返回过滤后的结果，格式如下:
            [
                {{"content": "相关内容1", "score": 原始分数, "relevance": "说明为什么相关"}},
                {{"content": "相关内容2", "score": 原始分数, "relevance": "说明为什么相关"}}
            ]
            
            只返回JSON格式的结果，不要有其他文字说明。如果所有结果都不相关，返回空数组 []。
            """
            
            # 调用LLM服务进行过滤
            print(f"Prompt: {prompt}")
            # 获取LLM实例
            llm = self.llm_service.get_llm()
            # 使用LLM生成文本
            llm_response = await llm.generate(prompt)
            print(f"LLM response: {llm_response}")
            
            # 尝试解析JSON响应
            try:
                # 提取JSON部分（如果LLM返回了额外文本）
                json_str = llm_response.strip()
                if json_str.startswith("```json"):
                    json_str = json_str.split("```json")[1]
                if json_str.endswith("```"):
                    json_str = json_str.split("```")[0]
                    
                filtered_results = json.loads(json_str.strip())
                
                # 确保结果格式正确
                if isinstance(filtered_results, list):
                    # 移除可能的额外字段，保持与原始结果格式一致
                    standardized_results = []
                    for item in filtered_results:
                        if isinstance(item, dict) and "content" in item:
                            result = {
                                "content": item["content"],
                                "score": item.get("score", 0.5)
                            }
                            # 保留token_usage如果存在
                            if "token_usage" in results[0]:
                                result["token_usage"] = results[0]["token_usage"]
                            standardized_results.append(result)
                    
                    return standardized_results if standardized_results else results
                
            except Exception as json_error:
                print(f"Error parsing LLM response as JSON: {str(json_error)}")
                print(f"Raw LLM response: {llm_response}")
                
            # 如果解析失败，返回原始结果
            return results
            
        except Exception as e:
            print(f"Error filtering results with LLM: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            # 出错时返回原始结果
            return results
    
    async def retrieve(self, query: str, uuid: str = None) -> List[Dict[str, Any]]:
        """
        从OpenSearch中检索与查询相关的数据
        
        Args:
            query: 用户查询
            uuid: 会话ID(可选)
            
        Returns:
            检索结果列表
        """
        try:
            # 检查客户端连接
            if not self.client:
                print("OpenSearch client is not connected, attempting to reconnect...")
                self.client = self._connect_to_opensearch()
                if not self.client:
                    error_msg = "无法连接到OpenSearch服务器，请检查配置"
                    error_result = [{"content": error_msg, "score": 0.0, "source": "opensearch"}]
                    
                    # 如果提供了UUID，将结果缓存
                    if uuid:
                        try:
                            key = f"{uuid}:opensearch"
                            self.redis_tools.set(key, error_result)
                            # 同时更新主缓存
                            cached_data = self.redis_tools.get(uuid) or {}
                            cached_data["opensearch"] = error_result
                            self.redis_tools.set(uuid, cached_data)
                            print(f"Cached OpenSearch error result for {uuid}")
                        except Exception as cache_error:
                            print(f"Error caching OpenSearch error result: {str(cache_error)}")
                    
                    return error_result
            
            # 记录开始时的token使用情况
            start_usage = self.llm_service.get_token_usage()
            
            # 构建并打印prompt
            prompt = f"OpenSearch检索查询:\n{query}"
            print(prompt)
            
            # 调用LLM服务获取查询意图
            intent_analysis = {}
            async for result in self.llm_service.identify_column(query):
                if isinstance(result, dict):
                    if "step" not in result:
                        intent_analysis = result
            print(f"Identified terms: {intent_analysis}")
            
            results = []
            
            # 如果识别出了意图，对每个关键术语进行搜索
            if intent_analysis:
                for key, term in intent_analysis.items():
                    print(f"Searching for term: {term}")
                    term_results = await self._search_term(term)
                    results.extend(term_results)
            
            # 如果没有识别出意图或没有结果，直接使用原始查询
            if not intent_analysis or not results:
                print(f"No intent identified or no results, using original query: {query}")
                direct_results = await self._search_term(query)
                results.extend(direct_results)
            
            # 如果有3个以上结果，使用LLM过滤
            if len(results) > 3:
                results = await self._filter_results_with_llm(query, results)
            
            # 记录结束时的token使用情况
            end_usage = self.llm_service.get_token_usage()
            
            # 计算token使用量
            input_tokens = end_usage["input_tokens"] - start_usage["input_tokens"]
            output_tokens = end_usage["output_tokens"] - start_usage["output_tokens"]
            
            # 打印token使用情况
            print(f"[OpenSearch Retriever] Total token usage - Input: {input_tokens} tokens, Output: {output_tokens} tokens")
            
            # 更新token计数器
            self.token_counter.total_input_tokens += input_tokens
            self.token_counter.total_output_tokens += output_tokens
            
            # 记录调用历史
            call_record = {
                "source": "opensearch-retriever",
                "input_tokens": input_tokens,
                "output_tokens": output_tokens
            }
            self.token_counter.calls_history.append(call_record)
            
            # 确保结果不为空
            if not results:
                results = [{"content": "未找到相关的存储过程或SQL代码", "score": 0.0, "source": "opensearch"}]
            
            # 为结果添加元数据
            for result in results:
                result["source"] = "opensearch"
                result["token_usage"] = {
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens
                }
            
            # 如果提供了UUID，将结果缓存
            if uuid:
                try:
                    # 使用专用key
                    key = f"{uuid}:opensearch"
                    self.redis_tools.set(key, results)
                    
                    # 同时更新主缓存
                    cached_data = self.redis_tools.get(uuid) or {}
                    cached_data["opensearch"] = results
                    self.redis_tools.set(uuid, cached_data)
                    print(f"Cached OpenSearch results for {uuid}")
                except Exception as cache_error:
                    print(f"Error caching OpenSearch results: {str(cache_error)}")
            
            return results
            
        except Exception as e:
            error_msg = f"OpenSearch检索错误: {str(e)}"
            print(error_msg)
            print(traceback.format_exc())
            
            error_result = [{"content": error_msg, "score": 0.0, "source": "opensearch"}]
            
            # 如果提供了UUID，将错误结果缓存
            if uuid:
                try:
                    key = f"{uuid}:opensearch"
                    self.redis_tools.set(key, error_result)
                    # 同时更新主缓存
                    cached_data = self.redis_tools.get(uuid) or {}
                    cached_data["opensearch"] = error_result
                    self.redis_tools.set(uuid, cached_data)
                except Exception:
                    pass  # 忽略缓存错误
            
            return error_result