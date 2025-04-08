from typing import List, Dict, Any
from Retriever.base_retriever import BaseRetriever
from service.neo4j_service import Neo4jService
from service.llm_service import LLMService
from tools.redis_tools import RedisTools
from tools.token_counter import TokenCounter
import json

class Neo4jRetriever(BaseRetriever):
    """Neo4j检索器，用于从Neo4j图数据库检索数据"""
    
    def __init__(self):
        """初始化Neo4j检索器"""
        self.neo4j_service = Neo4jService()
        self.llm_service = LLMService()
        self.redis_tools = RedisTools()
        # 初始化token计数器
        self.token_counter = TokenCounter()
    
    async def _query_relationships(self, term: str) -> List[Dict[str, Any]]:
        """
        查询与指定术语相关的所有关系
        
        Args:
            term: 要查询的术语
            
        Returns:
            包含关系信息的列表
        """
        try:
            # 首先尝试查询关系的所有属性，以便我们可以诊断问题
            debug_query = """
            MATCH (a:Table)-[r:RELATED_TO]->(b:Table)
            WHERE a.name = $term OR b.name = $term
            RETURN 
                a.name as source_table,
                b.name as target_table,
                properties(r) as relationship_properties,
                type(r) as relationship_type
            LIMIT 1
            """
            
            debug_results = self.neo4j_service.neo4j.execute_query(debug_query, parameters={"term": term})
            if debug_results:
                for record in debug_results:
                    print(f"DEBUG - Relationship properties for term '{term}': {record.get('relationship_properties')}")
                    print(f"DEBUG - Relationship type: {record.get('relationship_type')}")
            
            # 修改后的Cypher查询，尝试获取更多可能的字段名
            cypher_query = """
            MATCH (a:Table)-[r:RELATED_TO]->(b:Table)
            WHERE a.name = $term OR b.name = $term
            RETURN 
                a.name as source_table,
                b.name as target_table,
                r.source_field as source_field,
                r.target_field as target_field,
                r.sourceField as source_field_alt,
                r.targetField as target_field_alt,
                r.from_field as from_field,
                r.to_field as to_field,
                r.relationship_description as description,
                r.relationshipDescription as description_alt,
                r.created_at as created_at,
                r.createdAt as created_at_alt
            ORDER BY COALESCE(r.created_at, r.createdAt) DESC
            """
            
            # 执行查询
            results = self.neo4j_service.neo4j.execute_query(cypher_query, parameters={"term": term})
            print(f"Neo4j query results for term '{term}': {results}")
            
            # 格式化结果
            formatted_results = []
            for record in results:
                try:
                    # 打印完整记录以便诊断
                    print(f"DEBUG - Record: {record}")
                    
                    # 获取表名
                    source_table = record.get('source_table', 'Unknown')
                    target_table = record.get('target_table', 'Unknown')
                    
                    # 依次尝试不同的字段名以获取源字段
                    source_field = (
                        record.get('source_field') or 
                        record.get('source_field_alt') or 
                        record.get('from_field') or 
                        'Unknown'
                    )
                    
                    # 依次尝试不同的字段名以获取目标字段
                    target_field = (
                        record.get('target_field') or 
                        record.get('target_field_alt') or 
                        record.get('to_field') or 
                        'Unknown'
                    )
                    
                    # 获取描述和创建时间
                    description = record.get('description') or record.get('description_alt') or ''
                    created_at = record.get('created_at') or record.get('created_at_alt') or ''
                    
                    content_message = "表 {} 通过字段 {} 关联到表 {} 的字段 {}".format(
                        source_table, source_field, target_table, target_field
                    )
                    
                    formatted_results.append({
                        "content": content_message,
                        "description": description,
                        "created_at": created_at,
                        "score": 1.0
                    })
                except Exception as record_error:
                    print(f"Error processing record: {record}, Error: {str(record_error)}")
                    continue
            
            if not formatted_results:
                print(f"No relationships found for term '{term}'")
                
                # 如果没有结果，尝试查询数据库中存在的表
                table_query = """
                MATCH (t:Table)
                WHERE t.name CONTAINS $term
                RETURN t.name as table_name
                LIMIT 5
                """
                
                table_results = self.neo4j_service.neo4j.execute_query(table_query, parameters={"term": term})
                if table_results:
                    tables = [record.get('table_name') for record in table_results if record.get('table_name')]
                    if tables:
                        formatted_results.append({
                            "content": f"找到包含'{term}'的表: {', '.join(tables)}，但未找到相关关系",
                            "description": "找到表但无关系",
                            "score": 0.7
                        })
            
            return formatted_results
            
        except Exception as e:
            print(f"Error querying relationships for term '{term}': {str(e)}")
            print(f"Full error details: {e.__class__.__name__}: {str(e)}")
            return []
    
    async def retrieve(self, query: str, uuid: str = None) -> List[Dict[str, Any]]:
        """
        根据用户查询检索相关关系并缓存结果
        
        Args:
            query: 用户的查询字符串
            uuid: 用户的UUID，用于缓存结果
            
        Returns:
            包含相关关系信息的列表
        """
        try:
            # 记录开始时的token使用情况
            start_usage = self.llm_service.get_token_usage()
            
            # 构建并打印prompt
            prompt = f"Neo4j检索查询:\n{query}"
            print(prompt)
            
            # 调用LLM分析查询，识别查询意图
            # print(f"Analyzing query: {query}")
            intent_analysis = await self.llm_service.identify_column(query)
            # print(f"Identified intent: {intent_analysis}")
            
            all_results = []
            
            # 如果识别出了意图，对每个关键术语进行搜索
            if intent_analysis:
                for key, term in intent_analysis.items():
                    # print(f"Searching for term: {term}")
                    relationship_results = await self._query_relationships(term)
                    all_results.extend(relationship_results)
            else:
                # 如果没有识别出意图，直接使用原始查询
                # print(f"No intent identified, using original query: {query}")
                all_results.extend(await self._query_relationships(query))
            
            # 记录结束时的token使用情况
            end_usage = self.llm_service.get_token_usage()
            
            # 计算本次调用消耗的token
            input_tokens = end_usage["input_tokens"] - start_usage["input_tokens"]
            output_tokens = end_usage["output_tokens"] - start_usage["output_tokens"]
            
            # 打印token使用情况
            print(f"[Neo4j Retriever] Total token usage - Input: {input_tokens} tokens, Output: {output_tokens} tokens")
            
            # 如果没有结果，返回未找到的消息
            if not all_results:
                result_message = [{"content": "", "score": 0, "source": "neo4j", "token_usage": {"input_tokens": input_tokens, "output_tokens": output_tokens}}]
            else:
                # 添加token使用信息到结果中
                for result in all_results:
                    result["token_usage"] = {
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens
                    }
                result_message = all_results
            
            # 如果提供了UUID，将结果缓存到Redis
            if uuid:
                try:
                    # 获取现有的缓存数据
                    cached_data = self.redis_tools.get(uuid) or {}
                    
                    # 直接保存all_results，如果为空则保存空字符串
                    if all_results and len(all_results) > 0:
                        cached_data["neo4j"] = all_results
                    else:
                        cached_data["neo4j"] = ""
                    
                    # 更新Redis缓存
                    self.redis_tools.set(uuid, cached_data)
                    print(f"Cached Neo4j results for UUID: {uuid}")
                except Exception as cache_error:
                    print(f"Error caching Neo4j results: {str(cache_error)}")
            
            # print("Neo4j检索结果:", result_message)
            
            return result_message
            
        except Exception as e:
            # print(f"Neo4j retrieval error: {str(e)}")
            
            # 如果提供了UUID，将空结果存储到Redis
            if uuid:
                try:
                    # 获取现有的缓存数据
                    cached_data = self.redis_tools.get(uuid) or {}
                    # 设置空字符串
                    cached_data["neo4j"] = ""
                    # 更新Redis缓存
                    self.redis_tools.set(uuid, cached_data)
                except Exception:
                    pass
                
            return [{"content": "查询Neo4j时发生错误", "score": 0, "source": "neo4j"}]
        finally:
            try:
                self.neo4j_service.close()
            except Exception as close_error:
                # print(f"Error closing Neo4j connection: {str(close_error)}")
                pass