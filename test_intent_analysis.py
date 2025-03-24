"""
Test script for intent analysis with multilingual support
"""
import asyncio
from service.llm_service import LLMService
import config


async def test_intent_analysis():
    # Initialize LLM service
    llm_service = LLMService()
    
    # Initialize with Azure OpenAI
    llm_service.init_llm("azure-gpt4")
    
    # Test cases in different languages
    test_queries = [
        # English queries
        "What information is in the employee table?",
        "Show me the relationship between customers and orders",
        
        # Japanese queries
        "従業員テーブルの情報は何ですか？",
        "顧客と注文の関係を教えてください",
        
        # Mixed language
        "顧客テーブルのcustomer_id情報を教えてください",
        
        # Query with no clear table/column references
        "How are you doing today?"
    ]
    
    print("\n===== Testing intent analysis with multilingual queries =====\n")
    
    for query in test_queries:
        print(f"\nTesting query: \"{query}\"")
        try:
            intent_result = await llm_service.identify_column(query)
            print(f"Intent analysis result: {intent_result}")
        except Exception as e:
            print(f"Error analyzing intent: {str(e)}")


if __name__ == "__main__":
    asyncio.run(test_intent_analysis()) 