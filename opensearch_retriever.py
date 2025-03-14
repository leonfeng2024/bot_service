#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import json
import sys
import os
from opensearchpy import OpenSearch
from service.embedding_service import EmbeddingService

class OpenSearchTest:
    """OpenSearch测试类，专门用于测试KNN查询"""
    
    def __init__(self):
        # OpenSearch connection settings
        self.opensearch_host = "localhost"
        self.opensearch_port = 9200
        self.opensearch_user = "admin"
        self.opensearch_password = "admin"
        self.use_ssl = False
        self.procedure_index = "procedure_index"
        self.vector_dim = 1024
        
        # Initialize embedding service for query vectorization
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
            print("Successfully connected to OpenSearch")
            return client
        except Exception as e:
            print(f"Error connecting to OpenSearch: {str(e)}")
            print("Make sure OpenSearch is running and configuration is correct.")
            return None
    
    async def test_knn_query(self, term="employee_details"):
        """使用指定词语进行KNN查询测试"""
        if not self.client:
            print("OpenSearch client not initialized")
            return
        
        try:
            # 检查索引是否存在
            if not self.client.indices.exists(index=self.procedure_index):
                print(f"Index {self.procedure_index} does not exist.")
                return
            
            # 获取搜索词的向量表示
            print(f"Generating embedding for term: {term}")
            search_embedding = await self.embedding_service.get_embedding(term)
            
            # 获取实际的embedding维度
            self.vector_dim = len(search_embedding)
            print(f"Search term: {term}, embedding dimension: {self.vector_dim}")
            
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
                        "minimum_should_match": 0  # 设为0表示should条件不是必须的
                    }
                }
            }
            
            # 执行KNN查询
            print("\n=== 执行纯KNN查询 ===")
            knn_response = self.client.search(
                body=knn_query,
                index=self.procedure_index
            )
            self._print_results(knn_response, "KNN查询")
            
            # 执行带过滤的KNN查询
            print("\n=== 执行带过滤的KNN查询 ===")
            filtered_response = self.client.search(
                body=filtered_query,
                index=self.procedure_index
            )
            self._print_results(filtered_response, "带过滤的KNN查询")
            
            # 3. 执行纯文本匹配查询 (作为对比)
            print("\n=== 执行纯文本匹配查询 ===")
            match_query = {
                "size": 5,
                "_source": ["procedure_name", "sql_content", "table_name", "view_name"],
                "query": {
                    "multi_match": {
                        "query": term,
                        "fields": ["procedure_name", "sql_content", "table_name", "view_name"]
                    }
                }
            }
            match_response = self.client.search(
                body=match_query,
                index=self.procedure_index
            )
            self._print_results(match_response, "文本匹配查询")
            
        except Exception as e:
            print(f"Error during KNN test: {str(e)}")
            import traceback
            print(traceback.format_exc())
    
    def _print_results(self, response, query_type):
        """打印查询结果"""
        hits = response.get("hits", {}).get("hits", [])
        print(f"Found {len(hits)} results for {query_type}")
        
        for i, hit in enumerate(hits, 1):
            source = hit.get("_source", {})
            score = hit.get("_score", 0)
            
            print(f"\n结果 #{i} (score: {score}):")
            print(f"Procedure: {source.get('procedure_name', 'Unknown')}")
            
            sql_content = source.get("sql_content", "")
            # 如果SQL内容很长，只打印前200个字符
            if len(sql_content) > 200:
                print(f"SQL Content: {sql_content[:200]}... (truncated)")
            else:
                print(f"SQL Content: {sql_content}")
            
            table_name = source.get("table_name", "")
            if table_name:
                print(f"Table: {table_name}")
            
            view_name = source.get("view_name", "")
            if view_name:
                print(f"View: {view_name}")
            
            # 添加与搜索词相关的说明（与OpenSearchRetriever一致）
            print(f"\n与employee_details相关的procedure是：{sql_content}")
    
    def get_index_mapping(self):
        """获取索引映射信息"""
        if not self.client:
            print("OpenSearch client not initialized")
            return
        
        if not self.client.indices.exists(index=self.procedure_index):
            print(f"Index {self.procedure_index} does not exist.")
            return
        
        mapping = self.client.indices.get_mapping(index=self.procedure_index)
        print(f"Index mapping for {self.procedure_index}:")
        print(json.dumps(mapping, indent=2))

async def main():
    """主函数"""
    test = OpenSearchTest()
    
    # 先检查索引映射
    test.get_index_mapping()
    
    # 使用employee_details进行KNN查询测试
    await test.test_knn_query("employee_details")
    
    # 可选：测试其他关键词
    # await test.test_knn_query("employees")
    # await test.test_knn_query("department")

if __name__ == "__main__":
    # 确保正确导入service模块
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # 运行异步主函数
    asyncio.run(main()) 