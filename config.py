"""
配置文件，包含所有服务所需的配置项
"""

# Azure OpenAI 配置
AZURE_OPENAI_MODEL_NAME = "gpt-4o"
AZURE_OPENAI_API_BASE = "https://openai-southus-trng-app-chatbot01.openai.azure.com/"
AZURE_OPENAI_API_KEY = "46dcbd5837264b38988991c0a786ca6a"
AZURE_OPENAI_API_VERSION = "2024-02-15-preview"

# Claude 配置
CLAUDE_API_KEY = "your_claude_api_key_here"
CLAUDE_MODEL_NAME = "claude-3-sonnet-20240229"

# PostgreSQL 配置
POSTGRESQL_DBNAME = "local_rag"
POSTGRESQL_USER = "postgres"
POSTGRESQL_PASSWORD = "Automation2025"
POSTGRESQL_HOST = "localhost"
POSTGRESQL_PORT = "5432"

# Neo4j 配置
NEO4J_HOST = "localhost"
NEO4J_BOLT_PORT = 7687
NEO4J_HTTP_PORT = 7474
NEO4J_URI = f"bolt://{NEO4J_HOST}:{NEO4J_BOLT_PORT}"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "neo4j2025"

# Redis Configuration
REDIS_HOST = "localhost"  # 本地Docker容器，通过localhost访问
REDIS_PORT = 6379
REDIS_PASSWORD = "12345678"  # 本地容器需要密码认证
REDIS_DATABASE = 0
REDIS_TIMEOUT = 3000
REDIS_CONNECT_TIMEOUT = 3000
REDIS_CLIENT_NAME = "automation-platform"

# 生产环境配置 (可通过环境变量覆盖)
import os
if os.getenv("REDIS_ENV") == "production":
    REDIS_HOST = "coe_redis"
    REDIS_PASSWORD = "12345678"

# JWT 配置
JWT_SECRET = "your_jwt_secret_key"
JWT_EXPIRATION = 3600  # 1小时过期 