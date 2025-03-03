import asyncio
from service.rag_service import RAGService

async def test():
    rag = RAGService()
    
    # Test different queries
    queries = [
        "数据库中有哪些表？",
        "表 m_product 的结构是什么？",
        "表 m_product 的数据有哪些？",
        "什么是PostgreSQL？",
        "使用LLM解释PostgreSQL和Neo4j的区别"
    ]
    
    for query in queries:
        print(f"\n查询: {query}")
        results = await rag.retrieve(query)
        print(f"结果: {results}")

if __name__ == "__main__":
    asyncio.run(test()) 