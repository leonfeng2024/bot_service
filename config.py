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