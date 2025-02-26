from abc import ABC, abstractmethod
from typing import Optional
from anthropic import Anthropic
from openai import AzureOpenAI
from utils.singleton import singleton
from dotenv import load_dotenv
import os

load_dotenv()

class BaseLLM(ABC):
    @abstractmethod
    async def generate(self, prompt: str) -> str:
        pass

class Claude(BaseLLM):
    def __init__(self, api_key: str):
        self.client = Anthropic(api_key=api_key)
        self.model = os.getenv("CLAUDE_MODEL_NAME", "claude-3-sonnet-20240229")

    async def generate(self, prompt: str) -> str:
        message = await self.client.messages.create(
            model=self.model,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}]
        )
        return message.content

class AzureGPT4(BaseLLM):
    def __init__(self, api_key: str, api_base: str, api_version: str):
        self.client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=api_base,
            api_version=api_version
        )
        self.model = os.getenv("AZURE_OPENAI_MODEL_NAME", "gpt-4o")

    async def generate(self, prompt: str) -> str:
        completion = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            stream=False
        )
        return completion.choices[0].message.content

@singleton
class LLMService:
    def __init__(self):
        self.llm_instance: Optional[BaseLLM] = None

    def init_llm(self, llm_type: str, **kwargs) -> BaseLLM:
        if llm_type == "claude":
            self.llm_instance = Claude(api_key=kwargs.get("api_key"))
        elif llm_type == "azure-gpt4":
            self.llm_instance = AzureGPT4(
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                api_base=os.getenv("AZURE_OPENAI_API_BASE"),
                api_version=os.getenv("AZURE_OPENAI_API_VERSION")
            )
        else:
            raise ValueError(f"Unsupported LLM type: {llm_type}")

        return self.llm_instance

    def get_llm(self) -> BaseLLM:
        if not self.llm_instance:
            raise RuntimeError("LLM instance not initialized")
        return self.llm_instance