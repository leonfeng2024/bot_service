import asyncio
from service.rag_service import PostgreSQLRetriever

async def test():
    retriever = PostgreSQLRetriever()
    
    # Test query 1: List all tables
    print("=== Test Query 1: List all tables ===")
    result1 = await retriever.retrieve('列出所有表')
    print(result1)
    
    # Test query 2: Show the structure of a specific table
    print("\n=== Test Query 2: Show the structure of a table ===")
    result2 = await retriever.retrieve('显示表 m_product 的结构')
    print(result2)
    
    # Test query 3: Show sample data from a table
    print("\n=== Test Query 3: Show sample data from a table ===")
    result3 = await retriever.retrieve('显示表 m_product 的数据')
    print(result3)

if __name__ == "__main__":
    asyncio.run(test()) 