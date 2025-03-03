"""
配置文件，包含所有服务所需的配置项
"""

# Azure OpenAI 配置
AZURE_OPENAI_MODEL_NAME = "gpt-4o-mini"
AZURE_OPENAI_API_BASE = "https://traingpt.openai.azure.com/"
AZURE_OPENAI_API_KEY = ""
AZURE_OPENAI_API_VERSION = "2024-08-01-preview"

# Claude 配置
CLAUDE_API_KEY = "your_claude_api_key_here"
CLAUDE_MODEL_NAME = "claude-3-sonnet-20240229"

# PostgreSQL 配置
POSTGRESQL_DBNAME = "test_rag"
POSTGRESQL_USER = "postgres"
POSTGRESQL_PASSWORD = "Automation2025"
POSTGRESQL_HOST = "localhost"
POSTGRESQL_PORT = "5432" 