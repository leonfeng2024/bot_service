"""
Test the embedding service with the modified model loading logic.
"""
import asyncio
from service.embedding_service import EmbeddingService


async def test_embedding():
    # Initialize the Embedding service
    print("Initializing embedding service...")
    embedding_service = EmbeddingService()
    
    try:
        # Test getting an embedding
        text = "This is a test sentence to generate an embedding."
        print(f"Generating embedding for: '{text}'")
        
        embedding = await embedding_service.get_embedding(text)
        
        # Print embedding summary (first 10 elements and length)
        print(f"Embedding length: {len(embedding)}")
        print(f"Embedding preview (first 10 elements): {embedding[:10]}")
        
        print("Embedding service test successful!")
    except Exception as e:
        print(f"Error testing embedding service: {str(e)}")
        import traceback
        print(traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(test_embedding()) 