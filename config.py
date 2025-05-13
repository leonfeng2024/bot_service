"""
Configuration file containing all required service configurations
"""

# Azure OpenAI 
AZURE_OPENAI_MODEL_NAME = "gpt-4o"
AZURE_OPENAI_API_BASE = "https://openai-southus-trng-app-chatbot01.openai.azure.com/"
AZURE_OPENAI_API_KEY = ""
AZURE_OPENAI_API_VERSION = "2024-02-15-preview"

# OpenAI GPT-4.1 
OPENAI_API_KEY = ""
OPENAI_PROJECT_ID = "proj_VRqlWoHHTIFracPfGLymyGQq"
OPENAI_MODEL_NAME = "gpt-4.1"

# Claude 
CLAUDE_API_KEY = "your_claude_api_key_here"
CLAUDE_MODEL_NAME = "claude-3-sonnet-20240229"

# PostgreSQL 
POSTGRESQL_DBNAME = "local_rag"
POSTGRESQL_USER = "postgres"
POSTGRESQL_PASSWORD = "Automation2025"
POSTGRESQL_HOST = "bibot_postgres"
POSTGRESQL_PORT = "5432"

# Neo4j 
NEO4J_HOST = "bibot_neo4j"
NEO4J_BOLT_PORT = 7687
NEO4J_HTTP_PORT = 7474
NEO4J_URI = f"bolt://{NEO4J_HOST}:{NEO4J_BOLT_PORT}"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "neo4j2025"

# Redis Configuration
REDIS_HOST = "bibot_redis"  # bibot_redis 
REDIS_PORT = 6379
REDIS_PASSWORD = "12345678"  
REDIS_DATABASE = 0
REDIS_TIMEOUT = 3000
REDIS_CONNECT_TIMEOUT = 3000
REDIS_CLIENT_NAME = "automation-platform"

import os
if os.getenv("REDIS_ENV") == "production":
    REDIS_HOST = "coe_redis"
    REDIS_PASSWORD = "12345678"

# JWT 
JWT_SECRET = "your_jwt_secret_key"
JWT_EXPIRATION = 3600

# OpenSearch 
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "bibot_opensearch")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", 9200))
OPENSEARCH_USER = os.getenv("OPENSEARCH_USER", "admin")
OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD", "admin")
OPENSEARCH_USE_SSL = os.getenv("OPENSEARCH_USE_SSL", "False").lower() in ("true", "1", "yes")
OPENSEARCH_VERIFY_CERTS = os.getenv("OPENSEARCH_VERIFY_CERTS", "False").lower() in ("true", "1", "yes")