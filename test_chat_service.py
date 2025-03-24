"""
Test script for chat service
"""
import asyncio
from service.chat_service import ChatService


async def test_chat_service():
    # Initialize the chat service
    chat_service = ChatService()
    
    # Test cases
    test_queries = [
        "What is the impact of changing the customer_id column from VARCHAR(10) to VARCHAR(20)?",
        "Can you show me the dependencies of the orders table?",
        "What tables are related to the product table?",
        "リージョン別ランキングに関連するテーブルを教えてください"  # Japanese query
    ]
    
    print("\n===== Testing Chat Service =====\n")
    
    for query in test_queries:
        print(f"\nTesting query: \"{query}\"")
        try:
            response = await chat_service.handle_chat("user", query)
            print(f"Chat response: {response}")
        except Exception as e:
            print(f"Error in chat service: {str(e)}")


if __name__ == "__main__":
    asyncio.run(test_chat_service()) 