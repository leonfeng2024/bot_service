import asyncio
from service.rag_service import RAGService

async def test_rag_service():
    # 实例化RAGService
    rag_service = RAGService()
    
    # 测试查询
    query = "测试查询"
    uuid = "test-uuid-12345"
    
    # 测试_multi_source_retrieve方法返回格式
    print("Testing _multi_source_retrieve method:")
    results = await rag_service._multi_source_retrieve(query, uuid)
    print(f"Type of results: {type(results)}")
    print(f"Number of results: {len(results)}")
    for i, result in enumerate(results):
        if i < 3:  # 只打印前3条结果
            print(f"{i+1}. {result}")
        else:
            break
    print("...")
    
    # 测试完整检索流程
    print("\nTesting full retrieve method:")
    response = await rag_service.retrieve(query, uuid)
    print(f"Response status: {response.get('status')}")
    llm_response = response.get('message', {})
    print(f"LLM answer: {llm_response.get('answer', '')[:100]}..." if llm_response and isinstance(llm_response, dict) else "No answer")
    
    print("\nTest completed.")

if __name__ == "__main__":
    asyncio.run(test_rag_service()) 