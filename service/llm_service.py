from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Tuple
import json
from anthropic import Anthropic
from openai import AzureOpenAI
from pydantic import SecretStr
from utils.singleton import singleton
import os
import sys
from pathlib import Path
from langchain_openai import AzureChatOpenAI
import config
from tools.token_counter import TokenCounter

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
        self.token_counter = TokenCounter()

    async def generate(self, prompt: str) -> str:
        try:
            message = self.client.messages.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1000
            )
            res = str(message.content)
            
            # 计算并记录token使用情况
            self.token_counter.log_tokens(self.model, prompt, res, source="claude")
            
            return res if res is not None else "Empty Response Error"
        except Exception as e:
            import traceback
            return "LLM Error"

class AzureGPT4(BaseLLM):
    def __init__(self, api_key: str, api_base: str, api_version: str):
        self.client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=api_base,
            api_version=api_version
        )
        self.model = config.AZURE_OPENAI_MODEL_NAME
        self.token_counter = TokenCounter()

    async def generate(self, prompt: str) -> str:
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                stream=False
            )
            # 确保返回内容不为空，如果为空则返回错误信息
            content = completion.choices[0].message.content
            
            # 计算并记录token使用情况
            self.token_counter.log_tokens(self.model, prompt, content, source="azure-gpt4")
            
            return content if content is not None else "Empty Response Error"
        except Exception as e:
            import traceback
            return "LLM Error"


@singleton
class LLMService:
    def __init__(self):        
        self.llm_instance: Optional[BaseLLM] = None
        self.llm_agent_instance = None
        self.token_counter = TokenCounter()

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
                
            # 创建BaseCallbackHandler实例
            from langchain_core.callbacks import BaseCallbackHandler
            class TokenCallbackHandler(BaseCallbackHandler):
                def __init__(self, callback_func):
                    super().__init__()
                    self.callback_func = callback_func
                
                def on_llm_end(self, response, **kwargs):
                    """正确处理langchain的LLM结束回调"""
                    if hasattr(response, 'llm_output') and 'token_usage' in response.llm_output:
                        self.callback_func(token_usage=response.llm_output['token_usage'])
                    else:
                        self.callback_func(**kwargs)
            
            # 初始化AzureChatOpenAI
            self.llm_agent_instance = AzureChatOpenAI(
                api_key=SecretStr(api_key),
                azure_endpoint=config.AZURE_OPENAI_API_BASE,
                azure_deployment=config.AZURE_OPENAI_MODEL_NAME,
                api_version=config.AZURE_OPENAI_API_VERSION,
                callbacks=[TokenCallbackHandler(self._token_callback)]
            )
        else:
            raise ValueError(f"Unsupported LLM type: {llm_type}")

        return self.llm_agent_instance
    
    def _token_callback(self, **kwargs):
        """回调函数，用于记录langchain调用的token使用情况"""
        # 检查是否包含token使用信息
        if "token_usage" in kwargs:
            usage = kwargs["token_usage"]
            input_tokens = usage.get("prompt_tokens", 0)
            output_tokens = usage.get("completion_tokens", 0)
            
            # 记录token使用情况
            print(f"[Token Usage] langchain - Input: {input_tokens} tokens, Output: {output_tokens} tokens")
            
            # 更新总计数
            self.token_counter.total_input_tokens += input_tokens
            self.token_counter.total_output_tokens += output_tokens
            
            # 记录本次调用
            call_record = {
                "source": "langchain",
                "model": config.AZURE_OPENAI_MODEL_NAME,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens
            }
            self.token_counter.calls_history.append(call_record)

    def get_token_usage(self) -> Dict[str, Any]:
        """获取token使用情况"""
        return self.token_counter.get_total_usage()
    
    def get_formatted_token_usage(self) -> str:
        """获取格式化的token使用情况"""
        return self.token_counter.get_formatted_usage()

    async def identify_column(self, query: str) -> Dict[str, str]:
        """
        Analyze user query to extract relevant field names
        
        Args:
            query: User's query string
            
        Returns:
            Dict[str, str]: Dictionary of field names, format: {"item1": "field1", "item2": "field2"}
        """
        try:
            # Construct prompt with multilingual support
            prompt = f"""
You are a professional SQL database analysis assistant. You can analyze queries in any language, including English, Japanese, Chinese, etc.
Please analyze the user's query and extract the table names, view names, or field names they want to query.

User query: "{query}"

Please carefully analyze the query content and extract all possible database object names. These objects could be table names, view names, or column names.
For example, if the user asks "What fields are in the employee table?", you should extract "employee" as the key object.
If the user asks "I want to know the relationship between employee and department", you should extract "employee" and "department".
If the user asks in Japanese "従業員テーブルのフィールドは何ですか？", you should extract "従業員" as the key object.

Please return the analysis result in JSON format as follows:
{{
  "item1": "first extracted object name", 
  "item2": "second extracted object name",
  ... and so on
}}

If no clear object names are found, please return an empty JSON object {{}}.
IMPORTANT: Please ONLY return a valid JSON object. Do not include any other text before or after the JSON.
Do not include any explanations, preamble, or conclusion outside the JSON structure.

EXAMPLE
user_query : "please tell me something about change view column employees"
return {{"item1":"employees"}}

user_query : please tell me something about change table column employee_id"
return {{"item1":"employee_id"}}

"""

            # Get LLM instance and generate
            llm = self.get_llm()
            result = await llm.generate(prompt)
            
            # Strip any leading/trailing whitespace and non-JSON content
            result = result.strip()
            
            # Try to parse JSON result
            try:
                parsed_result = json.loads(result)
                return parsed_result
            except json.JSONDecodeError as json_err:
                # If result is not valid JSON, try to extract JSON part
                import re
                
                # Try to find anything that looks like a JSON object
                json_pattern = r'({[\s\S]*?})'
                json_matches = re.findall(json_pattern, result, re.DOTALL)
                
                for potential_json in json_matches:
                    try:
                        parsed_result = json.loads(potential_json)
                        return parsed_result
                    except json.JSONDecodeError:
                        continue
                
                # If extraction failed but result contains curly braces, try to manually fix common JSON errors
                if '{' in result and '}' in result:
                    # Extract content between first { and last }
                    start = result.find('{')
                    end = result.rfind('}') + 1
                    json_content = result[start:end]
                    
                    # Replace single quotes with double quotes
                    json_content = json_content.replace("'", '"')
                    
                    # Try parsing again
                    try:
                        parsed_result = json.loads(json_content)
                        return parsed_result
                    except json.JSONDecodeError:
                        pass
                
                # If all attempts failed, return empty result
                return {}
                
        except Exception as e:
            import traceback
            return {}
