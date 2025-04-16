#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test script to verify Redis, OpenSearch and PostgreSQL services
"""

import asyncio
import sys
import os
import uuid
import traceback

# Add project root to Python path if needed
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.redis_tools import RedisTools
from tools.opensearch_tools import OpenSearchTools, OpenSearchConfig
from tools.postgresql_tools import PostgreSQLTools
from Retriever.opensearch_retriever import OpenSearchRetriever
from Retriever.postgresql_retriever import PostgreSQLRetriever


async def test_redis_connection():
    """Test Redis connection and basic operations"""
    print("\n===== Testing Redis Connection =====")
    try:
        # Initialize Redis tools
        redis = RedisTools()
        
        # Test Redis connection
        test_key = f"test:{str(uuid.uuid4())}"
        test_value = {"test": "value", "timestamp": "2023-07-01"}
        
        # Set value
        success = redis.set(test_key, test_value)
        print(f"Redis set operation: {'Success' if success else 'Failed'}")
        
        # Get value
        retrieved = redis.get(test_key)
        print(f"Redis get operation: {'Success' if retrieved == test_value else 'Failed'}")
        print(f"Retrieved value: {retrieved}")
        
        # Delete value
        success = redis.delete(test_key)
        print(f"Redis delete operation: {'Success' if success else 'Failed'}")
        
        return True
    except Exception as e:
        print(f"Redis connection test failed: {str(e)}")
        print(traceback.format_exc())
        return False


async def test_opensearch_connection():
    """Test OpenSearch connection and basic operations"""
    print("\n===== Testing OpenSearch Connection =====")
    try:
        # Initialize OpenSearch tools
        os_tools = OpenSearchTools()
        
        # Test OpenSearch connection by listing indices
        print("Getting OpenSearch indices...")
        indices = os_tools.get_index_list()
        print(f"Found {len(indices)} indices:")
        for idx in indices:
            print(f"  - {idx}")
        
        # Test OpenSearch Retriever
        print("\nTesting OpenSearchRetriever...")
        os_retriever = OpenSearchRetriever()
        if os_retriever.client is None:
            print("ERROR: OpenSearchRetriever client is None!")
            print("Connection to OpenSearch failed")
            return False
        else:
            print("OpenSearchRetriever client initialized successfully")
        
        return True
    except Exception as e:
        print(f"OpenSearch connection test failed: {str(e)}")
        print(traceback.format_exc())
        return False


async def test_postgresql_connection():
    """Test PostgreSQL connection and basic operations"""
    print("\n===== Testing PostgreSQL Connection =====")
    try:
        # Initialize PostgreSQL tools
        pg_tools = PostgreSQLTools()
        
        # Test connection by getting available tables
        print("Getting PostgreSQL tables...")
        search_objects = pg_tools.get_search_objects()
        print(f"Found {len(search_objects)} tables/views:")
        for obj in search_objects[:10]:  # Show first 10 to avoid too much output
            print(f"  - {obj}")
        if len(search_objects) > 10:
            print(f"  ... and {len(search_objects) - 10} more")
        
        # Try a simple query
        print("\nExecuting simple query...")
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' LIMIT 5"
        results = pg_tools.execute_query(query)
        print(f"Query results: {results}")
        
        return True
    except Exception as e:
        print(f"PostgreSQL connection test failed: {str(e)}")
        print(traceback.format_exc())
        return False


async def test_service_queries():
    """Test actual service queries"""
    print("\n===== Testing Service Queries =====")
    test_uuid = str(uuid.uuid4())
    print(f"Test UUID: {test_uuid}")
    
    try:
        # Test Redis
        redis = RedisTools()
        
        # Test OpenSearch retriever query
        print("\nTesting OpenSearch retriever...")
        os_retriever = OpenSearchRetriever()
        query = "我想修改表字段employees请告诉我哪些内容会收到影响"
        
        try:
            os_results = await os_retriever.retrieve(query, test_uuid)
            print(f"OpenSearch retrieved {len(os_results)} results")
            if len(os_results) > 0:
                print(f"Sample result: {os_results[0].get('content', '')[:100]}...")
            
            # Check Redis storage
            stored_os = redis.get(f"{test_uuid}:opensearch")
            print(f"OpenSearch data in Redis: {'Found' if stored_os else 'Not found'}")
            if stored_os:
                if isinstance(stored_os, list) and len(stored_os) > 0:
                    print(f"Sample from Redis: {stored_os[0].get('content', '')[:100]}...")
        except Exception as e:
            print(f"OpenSearch retriever error: {str(e)}")
            print(traceback.format_exc())
        
        # Test PostgreSQL retriever query
        print("\nTesting PostgreSQL retriever...")
        pg_retriever = PostgreSQLRetriever()
        
        try:
            pg_results = await pg_retriever.retrieve(query, test_uuid)
            print(f"PostgreSQL retrieved {len(pg_results)} results")
            if len(pg_results) > 0:
                content = pg_results[0].get('content', '')
                print(f"Sample result: {content[:100]}...")
                print(f"Full length: {len(content)} characters")
            
            # Check Redis storage
            stored_pg = redis.get(f"{test_uuid}:postgresql")
            print(f"PostgreSQL data in Redis: {'Found' if stored_pg else 'Not found'}")
            if stored_pg:
                if isinstance(stored_pg, list) and len(stored_pg) > 0:
                    content = stored_pg[0].get('content', '')
                    print(f"Sample from Redis: {content[:100]}...")
                    print(f"Full Redis content length: {len(content)} characters")
        except Exception as e:
            print(f"PostgreSQL retriever error: {str(e)}")
            print(traceback.format_exc())
        
        return True
    except Exception as e:
        print(f"Service query tests failed: {str(e)}")
        print(traceback.format_exc())
        return False


async def main():
    """Run all tests"""
    print("Starting service tests...")
    
    redis_ok = await test_redis_connection()
    os_ok = await test_opensearch_connection()
    pg_ok = await test_postgresql_connection()
    
    if redis_ok and os_ok and pg_ok:
        print("\nBasic connection tests passed, testing service queries...")
        await test_service_queries()
    else:
        print("\nConnection tests failed, skipping service queries.")
    
    print("\nTests completed.")


if __name__ == "__main__":
    asyncio.run(main()) 