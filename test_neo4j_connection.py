"""
Test Neo4j connection using the updated service with config.py
"""
import asyncio
from service.neo4j_service import Neo4jService


async def test_connection():
    # Initialize the Neo4j service
    neo4j_service = Neo4jService()
    
    try:
        # Test a simple query to check connection
        node = await neo4j_service.create_node("TestNode", {"name": "test_connection", "timestamp": "now"})
        print(f"Created test node: {node}")
        
        # Get nodes to verify
        nodes = await neo4j_service.get_nodes("TestNode", {"name": "test_connection"})
        print(f"Retrieved nodes: {nodes}")
        
        print("Neo4j connection test successful!")
    except Exception as e:
        print(f"Error testing Neo4j connection: {str(e)}")
    finally:
        # Close the connection
        neo4j_service.close()


if __name__ == "__main__":
    asyncio.run(test_connection()) 