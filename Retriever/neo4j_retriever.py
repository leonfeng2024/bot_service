from typing import List, Dict, Any
from Retriever.base_retriever import BaseRetriever
from service.neo4j_service import Neo4jService
from service.llm_service import LLMService
import json

class Neo4jRetriever(BaseRetriever):
    """Neo4j检索器，用于从Neo4j图数据库检索数据"""
    
    def __init__(self):
        """初始化Neo4j检索器"""
        self.neo4j_service = Neo4jService()
        self.llm_service = LLMService()
    
    async def _query_relationships(self, term: str) -> List[Dict[str, Any]]:
        """
        查询与指定术语相关的所有关系
        
        Args:
            term: 要查询的术语
            
        Returns:
            包含关系信息的列表
        """
        try:
            # 构建查询以获取指定表/视图/存储过程的所有关系
            cypher_query = """
            MATCH (a:Table)-[r:RELATED_TO]->(b:Table)
            WHERE a.name = $term OR b.name = $term
            RETURN 
                a.name as source_table,
                b.name as target_table,
                r.source_field,
                r.target_field,
                r.relationship_description,
                r.created_at
            ORDER BY r.created_at DESC
            """
            
            # 执行查询
            results = self.neo4j_service.neo4j.execute_query(cypher_query, parameters={"term": term})
            
            # 格式化结果
            formatted_results = []
            for record in results:
                formatted_results.append({
                    "content": f"表 {record['source_table']} 通过字段 {record['source_field']} 关联到表 {record['target_table']} 的字段 {record['target_field']}",
                    "description": record['relationship_description'],
                    "created_at": record['created_at'],
                    "score": 1.0  # 由于是精确匹配，设置最高分数
                })
            
            return formatted_results
            
        except Exception as e:
            print(f"Error querying relationships for term '{term}': {str(e)}")
            return []
    
    async def retrieve(self, query: str) -> List[Dict[str, Any]]:
        """
        根据用户查询检索相关关系
        
        Args:
            query: 用户的查询字符串
            
        Returns:
            包含相关关系信息的列表
        """
        try:
            # 调用LLM分析查询，识别查询意图
            print(f"Analyzing query: {query}")
            intent_analysis = await self.llm_service.identify_column(query)
            print(f"Identified intent: {json.dumps(intent_analysis, ensure_ascii=False)}")
            
            all_results = []
            
            # 如果识别出了意图，对每个关键术语进行搜索
            if intent_analysis:
                for key, term in intent_analysis.items():
                    print(f"Searching for term: {term}")
                    relationship_results = await self._query_relationships(term)
                    all_results.extend(relationship_results)
            else:
                # 如果没有识别出意图，直接使用原始查询
                print(f"No intent identified, using original query: {query}")
                all_results.extend(await self._query_relationships(query))
            
            # 如果没有结果，返回未找到的消息
            if not all_results:
                return [{"content": f"未找到与查询 '{query}' 相关的数据库关系", "score": 0.89}]
            
            # 添加一个总结性的结果
            summary = {
                "content": f"查询 '{query}' 涉及以下数据库关系：",
                "score": 0.95
            }
            all_results.insert(0, summary)
            
            return all_results
            
        except Exception as e:
            print(f"Neo4j retrieval error: {str(e)}")
            return [{"content": f"查询Neo4j时发生错误: {str(e)}", "score": 0.89}]
        finally:
            self.neo4j_service.close() 