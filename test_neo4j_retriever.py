"""
测试Neo4j检索器，确保不会出现字符串格式化错误
"""
import asyncio
from Retriever.neo4j_retriever import Neo4jRetriever
from service.rag_service import RAGService

async def test_neo4j_retriever():
    # 初始化Neo4j检索器
    retriever = Neo4jRetriever()
    
    # 测试不同类型的查询
    test_queries = [
        "customer_id", 
        "order table",
        "リージョン別ランキング",  # 日语测试
        "不存在的表名",  # 不存在的表名
        "SELECT * FROM orders"  # SQL语句测试
    ]
    
    print("\n===== 测试Neo4j检索器 =====\n")
    
    for query in test_queries:
        print(f"\n测试查询: \"{query}\"")
        try:
            results = await retriever.retrieve(query)
            print(f"检索结果: {results}\n")
        except Exception as e:
            print(f"检索出错: {str(e)}\n")
    
    print("\n===== 测试RAG服务 =====\n")
    
    # 初始化RAG服务
    rag_service = RAGService()
    
    for query in test_queries:
        print(f"\n测试查询: \"{query}\"")
        try:
            results = await rag_service._multi_source_retrieve(query)
            # 只打印Neo4j相关的结果
            neo4j_results = [r for r in results if r.get('source') == 'neo4j']
            print(f"Neo4j检索结果: {neo4j_results}\n")
        except Exception as e:
            print(f"RAG服务检索出错: {str(e)}\n")

if __name__ == "__main__":
    asyncio.run(test_neo4j_retriever()) 