from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
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
            
            # Debugging: Print raw response
            print(f"Raw LLM response: {result}")
            
            # Strip any leading/trailing whitespace and non-JSON content
            result = result.strip()
            
            # Try to parse JSON result
            try:
                parsed_result = json.loads(result)
                return parsed_result
            except json.JSONDecodeError as json_err:
                print(f"JSON decode error: {str(json_err)}")
                
                # If result is not valid JSON, try to extract JSON part
                import re
                
                # Try to find anything that looks like a JSON object
                json_pattern = r'({[\s\S]*?})'
                json_matches = re.findall(json_pattern, result, re.DOTALL)
                
                for potential_json in json_matches:
                    try:
                        parsed_result = json.loads(potential_json)
                        print(f"Successfully extracted JSON: {potential_json}")
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
                        print(f"Manually fixed JSON: {json_content}")
                        return parsed_result
                    except json.JSONDecodeError:
                        print(f"Failed to fix JSON: {json_content}")
                
                # If all attempts failed, return empty result
                print(f"All JSON parsing attempts failed for: {result}")
                return {}
                
        except Exception as e:
            import traceback
            print(f"Error in identify_column: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            return {}
