from abc import ABC, abstractmethod
from typing import Optional
from anthropic import Anthropic
from openai import AzureOpenAI
from pydantic import SecretStr
from utils.singleton import singleton
import os
import sys
from pathlib import Path
from langchain_openai import AzureChatOpenAI
import config

ROOT_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = os.path.join(ROOT_DIR, '.env')

class BaseLLM(ABC):
    @abstractmethod
    async def generate(self, prompt: str) -> str:
        pass

class Claude(BaseLLM):
    def __init__(self, api_key: str):
        self.client = Anthropic(api_key=api_key)
        self.model = config.CLAUDE_MODEL_NAME

    async def generate(self, prompt: str) -> str:
        try:
            message = self.client.messages.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1000
            )
            res = str(message.content)
            return res if res is not None else "Empty Response Error"
        except Exception as e:
            import traceback
            print(f"Claude error: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            return "LLM Error"

class AzureGPT4(BaseLLM):
    def __init__(self, api_key: str, api_base: str, api_version: str):
        self.client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=api_base,
            api_version=api_version
        )
        self.model = config.AZURE_OPENAI_MODEL_NAME

    async def generate(self, prompt: str) -> str:
        try:
            print(prompt)
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                stream=False
            )
            # 确保返回内容不为空，如果为空则返回错误信息
            content = completion.choices[0].message.content
            return content if content is not None else "Empty Response Error"
        except Exception as e:
            import traceback
            print(f"Azure OpenAI error: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            return "LLM Error"


@singleton
class LLMService:
    def __init__(self):        
        self.llm_instance: Optional[BaseLLM] = None
        self.llm_agent_instance = None

    def init_llm(self, llm_type: str, **kwargs) -> BaseLLM:
        if llm_type == "claude":
            api_key = kwargs.get("api_key", config.CLAUDE_API_KEY)
            self.llm_instance = Claude(api_key=api_key)
        elif llm_type == "azure-gpt4":
            api_key = kwargs.get("api_key", config.AZURE_OPENAI_API_KEY)
            if not api_key:
                raise ValueError("AZURE_OPENAI_API_KEY not configured")
            
            self.llm_instance = AzureGPT4(
                api_key=api_key,
                api_base=config.AZURE_OPENAI_API_BASE,
                api_version=config.AZURE_OPENAI_API_VERSION
            )
        else:
            raise ValueError(f"Unsupported LLM type: {llm_type}")

        return self.llm_instance

    def get_llm(self) -> BaseLLM:
        if not self.llm_instance:
            raise RuntimeError("LLM instance not initialized")
        return self.llm_instance

    def init_agent_llm(self, llm_type: str):
        if llm_type == "azure-gpt4":
            api_key = "" if config.AZURE_OPENAI_API_KEY is None else config.AZURE_OPENAI_API_KEY
            if not api_key:
                raise ValueError("AZURE_OPENAI_API_KEY not configured")
                
            # 直接使用字符串而不是 SecretStr，避免可能的序列化问题
            self.llm_agent_instance = AzureChatOpenAI(
                api_key=SecretStr(api_key),
                azure_endpoint=config.AZURE_OPENAI_API_BASE,
                azure_deployment=config.AZURE_OPENAI_MODEL_NAME,
                api_version=config.AZURE_OPENAI_API_VERSION
            )
        else:
            raise ValueError(f"Unsupported LLM type: {llm_type}")

        return self.llm_agent_instance
