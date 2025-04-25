from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Tuple
import json
from anthropic import Anthropic
from openai import AzureOpenAI, OpenAI
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

# 全局LLM模型配置
class GlobalLLMConfig:
    DEFAULT_LLM_TYPE = "openai-gpt41"  # azure-gpt4  openai-gpt41 默认使用OpenAI GPT-4.1
    _current_llm_type = DEFAULT_LLM_TYPE
    _is_initialized = False
    _fallback_attempted = False
    
    @classmethod
    def get_current_llm_type(cls):
        if not cls._is_initialized:
            # 尝试预检查OpenAI可用性
            cls._check_openai_availability()
            cls._is_initialized = True
        return cls._current_llm_type
    
    @classmethod
    def set_current_llm_type(cls, llm_type: str):
        previous = cls._current_llm_type
        cls._current_llm_type = llm_type
        cls._is_initialized = True
        if previous != llm_type:
            print(f"全局LLM类型已从 {previous} 切换为 {llm_type}")
    
    @classmethod
    def reset(cls):
        cls._current_llm_type = cls.DEFAULT_LLM_TYPE
        cls._is_initialized = False
        cls._fallback_attempted = False
        print(f"全局LLM类型已重置为默认值: {cls.DEFAULT_LLM_TYPE}")
    
    @classmethod
    def _check_openai_availability(cls):
        """检查OpenAI是否可用，不可用则切换到Azure"""
        if cls._fallback_attempted:
            return  # 避免重复检查
        
        try:
            # 使用原生SDK测试连接
            api_key = config.OPENAI_API_KEY
            project_id = config.OPENAI_PROJECT_ID
            test_client = OpenAI(
                api_key=api_key,
            )
            # 简单测试请求（不传 headers 参数）
            test_client.models.list()
            # 如果成功，保持使用OpenAI
            print("OpenAI API可用性检查成功，使用OpenAI GPT-4.1")
            cls._current_llm_type = "openai-gpt41"
        except Exception as e:
            error_str = str(e).lower()
            # 检查是否应该切换到Azure
            if any([
                "403" in error_str,
                "401" in error_str, 
                "429" in error_str,
                "timeout" in error_str,
                "unsupported_country_region_territory" in error_str,
                "not_found" in error_str,
                "invalid_api_key" in error_str,
                "model not found" in error_str
            ]):
                print(f"OpenAI API不可用: {str(e)}")
                print("自动切换到Azure OpenAI")
                cls._current_llm_type = "azure-gpt4"
            cls._fallback_attempted = True


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
            # 如果Azure出错，记录但不切换全局模型
            print(f"Azure GPT-4 Error: {str(e)}")
            print(traceback.format_exc())
            return f"Azure LLM Error: {str(e)}"

class OpenAIGPT41(BaseLLM):
    def __init__(self, api_key: str, project_id: str):
        self.client = OpenAI(
            api_key=api_key,
        )
        self.model = "gpt-4.1"
        self.project_id = project_id  # Store project_id as an instance variable
        self.token_counter = TokenCounter()
        # 添加备用LLM
        self.fallback_llm = None
        # 标记是否已尝试过使用备用LLM
        self.fallback_attempted = False

    async def generate(self, prompt: str) -> str:
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                stream=False,
                extra_headers={
                    "OpenAI-Beta": "assistants=v1",
                    "OpenAI-Project": self.project_id
                }
            )
            # 确保返回内容不为空，如果为空则返回错误信息
            content = completion.choices[0].message.content
            
            # 计算并记录token使用情况
            self.token_counter.log_tokens(self.model, prompt, content, source="openai-gpt41")
            
            # 重置备用尝试标记
            self.fallback_attempted = False
            
            return content if content is not None else "Empty Response Error"
        except Exception as e:
            import traceback
            error_message = str(e)
            
            # 如果已经尝试过备用LLM，避免无限递归
            if self.fallback_attempted:
                print(f"已经尝试过备用LLM，避免无限递归")
                return f"所有LLM服务均失败: {error_message}"
                
            # 检查各种可能的OpenAI错误情况
            if (
                "403" in error_message or  # 权限错误
                "401" in error_message or  # 认证错误
                "429" in error_message or  # 速率限制
                "500" in error_message or  # 服务器错误
                "502" in error_message or  # 网关错误
                "503" in error_message or  # 服务不可用
                "504" in error_message or  # 网关超时
                "unsupported_country_region_territory" in error_message or  # 区域限制
                "not_found" in error_message or  # 资源不存在
                "invalid_api_key" in error_message or  # API密钥无效
                "model not found" in error_message.lower() or  # 模型不存在
                "timeout" in error_message.lower()  # 连接超时
            ):
                error_type = ""
                if "403" in error_message and "unsupported_country_region_territory" in error_message:
                    error_type = "区域限制错误"
                elif "403" in error_message:
                    error_type = "权限错误"
                elif "401" in error_message:
                    error_type = "认证错误"
                elif "429" in error_message:
                    error_type = "速率限制"
                elif "timeout" in error_message.lower():
                    error_type = "连接超时"
                elif "model not found" in error_message.lower():
                    error_type = "模型不存在"
                else:
                    error_type = "服务器错误"
                
                print(f"{error_type}: OpenAI API不可用。将回退到Azure OpenAI。")
                print(f"原始错误: {error_message}")
                
                # 设置全局LLM类型为Azure
                GlobalLLMConfig.set_current_llm_type("azure-gpt4")
                
                # 标记已尝试使用备用LLM
                self.fallback_attempted = True
                
                # 尝试使用Azure OpenAI作为备用
                try:
                    if self.fallback_llm is None:
                        # 初始化Azure备用LLM
                        self.fallback_llm = AzureGPT4(
                            api_key=config.AZURE_OPENAI_API_KEY,
                            api_base=config.AZURE_OPENAI_API_BASE,
                            api_version=config.AZURE_OPENAI_API_VERSION
                        )
                    
                    # 使用备用LLM
                    print(f"使用Azure OpenAI作为备用...")
                    fallback_response = await self.fallback_llm.generate(prompt)
                    return fallback_response
                except Exception as fallback_error:
                    print(f"备用LLM也失败: {str(fallback_error)}")
                    return f"OpenAI API不可用 ({error_type})，备用服务也失败: {str(fallback_error)}。请检查您的网络环境或配置。"
            
            print(f"OpenAI GPT-4.1 Error: {error_message}")
            print(traceback.format_exc())
            return f"LLM请求错误: {error_message}"


@singleton
class LLMService:
    def __init__(self):        
        self.llm_instance: Optional[BaseLLM] = None
        self.llm_agent_instance = None
        self.token_counter = TokenCounter()
        self.last_init_type = None

    def init_llm(self, llm_type: Optional[str] = None, **kwargs) -> BaseLLM:
        # 如果未指定llm_type，使用全局配置
        if llm_type is None:
            llm_type = GlobalLLMConfig.get_current_llm_type()
            print(f"使用全局LLM类型: {llm_type}")
        
        # 如果已经初始化了相同类型的LLM，直接返回
        if self.llm_instance is not None and self.last_init_type == llm_type:
            return self.llm_instance
            
        self.last_init_type = llm_type
        
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
        elif llm_type == "openai-gpt41":
            api_key = kwargs.get("api_key", config.OPENAI_API_KEY)
            project_id = kwargs.get("project_id", config.OPENAI_PROJECT_ID)
            if not api_key:
                raise ValueError("OPENAI_API_KEY not configured")
            if not project_id:
                raise ValueError("OPENAI_PROJECT_ID not configured")
            
            try:
                self.llm_instance = OpenAIGPT41(
                    api_key=api_key,
                    project_id=project_id
                )
            except Exception as e:
                # 如果OpenAI初始化失败，自动切换到Azure
                print(f"OpenAI初始化失败，自动切换到Azure: {str(e)}")
                GlobalLLMConfig.set_current_llm_type("azure-gpt4")
                return self.init_llm("azure-gpt4", **kwargs)
        else:
            raise ValueError(f"Unsupported LLM type: {llm_type}")

        return self.llm_instance

    def get_llm(self) -> BaseLLM:
        # 每次调用前检查当前的全局配置，如果与上次初始化不同，则重新初始化
        current_type = GlobalLLMConfig.get_current_llm_type()
        if self.last_init_type != current_type:
            print(f"检测到全局LLM类型变更: {self.last_init_type} -> {current_type}")
            self.init_llm(current_type)
        
        if not self.llm_instance:
            self.init_llm()
            
        if not self.llm_instance:
            raise RuntimeError("LLM instance not initialized")
            
        return self.llm_instance
        
    def force_reset_llm(self, llm_type: Optional[str] = None):
        """强制重置LLM实例，用于测试或手动切换"""
        self.llm_instance = None
        self.last_init_type = None
        if llm_type:
            GlobalLLMConfig.set_current_llm_type(llm_type)
        return self.init_llm()

    def init_agent_llm(self, llm_type: Optional[str] = None):
        # 如果未指定llm_type，使用全局配置
        if llm_type is None:
            llm_type = GlobalLLMConfig.get_current_llm_type()
            print(f"使用全局LLM类型初始化Agent: {llm_type}")
            
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
            
        if llm_type == "azure-gpt4":
            api_key = "" if config.AZURE_OPENAI_API_KEY is None else config.AZURE_OPENAI_API_KEY
            if not api_key:
                raise ValueError("AZURE_OPENAI_API_KEY not configured")
            
            # 初始化AzureChatOpenAI
            self.llm_agent_instance = AzureChatOpenAI(
                api_key=SecretStr(api_key),
                azure_endpoint=config.AZURE_OPENAI_API_BASE,
                azure_deployment=config.AZURE_OPENAI_MODEL_NAME,
                api_version=config.AZURE_OPENAI_API_VERSION,
                callbacks=[TokenCallbackHandler(self._token_callback)]
            )
        elif llm_type == "openai-gpt41":
            from langchain_openai import ChatOpenAI
            
            api_key = "" if config.OPENAI_API_KEY is None else config.OPENAI_API_KEY
            project_id = "" if config.OPENAI_PROJECT_ID is None else config.OPENAI_PROJECT_ID
            
            if not api_key:
                raise ValueError("OPENAI_API_KEY not configured")
            if not project_id:
                raise ValueError("OPENAI_PROJECT_ID not configured")
            
            # 定义错误处理函数
            def should_fallback_to_azure(error_message):
                """判断是否应该回退到Azure OpenAI"""
                error_str = str(error_message).lower()
                return any([
                    "403" in error_str,
                    "401" in error_str,
                    "429" in error_str,
                    "500" in error_str, 
                    "502" in error_str,
                    "503" in error_str,
                    "504" in error_str,
                    "unsupported_country_region_territory" in error_str,
                    "not_found" in error_str,
                    "invalid_api_key" in error_str,
                    "model not found" in error_str,
                    "timeout" in error_str,
                    "openai" in error_str and "error" in error_str,
                    "connection" in error_str and "failed" in error_str
                ])
            
            # 初始化ChatOpenAI 
            try:
                # 先尝试测试连接OpenAI
                try:
                    # 使用原生SDK测试连接
                    test_client = OpenAI(
                        api_key=api_key,
                    )
                    # 简单测试请求 - 这将触发任何连接或认证问题（不传 headers 参数）
                    test_client.models.list()
                    print("OpenAI连接测试成功，继续使用GPT-4.1")
                except Exception as test_error:
                    if should_fallback_to_azure(test_error):
                        print(f"OpenAI连接测试失败: {str(test_error)}")
                        print("直接切换到Azure OpenAI作为备用")
                        # 更新全局LLM类型
                        GlobalLLMConfig.set_current_llm_type("azure-gpt4")
                        # 递归调用自身，但使用Azure类型
                        return self.init_agent_llm("azure-gpt4")
                    else:
                        # 其他错误可能只是测试方法的问题，仍然尝试初始化
                        print(f"OpenAI连接测试出现非致命错误: {str(test_error)}")
                
                # 初始化LangChain ChatOpenAI
                self.llm_agent_instance = ChatOpenAI(
                    model=config.OPENAI_MODEL_NAME,
                    openai_api_key=config.OPENAI_API_KEY,
                    callbacks=[TokenCallbackHandler(self._token_callback)]
                )
                
                # 注册错误处理回调
                class ErrorCallbackHandler(BaseCallbackHandler):
                    def on_llm_error(self, error, **kwargs):
                        if should_fallback_to_azure(error):
                            print(f"LangChain OpenAI错误，将回退到Azure: {str(error)}")
                            # 更新全局LLM类型
                            GlobalLLMConfig.set_current_llm_type("azure-gpt4")
                            raise ValueError(f"Fallback to Azure: {str(error)}")
                        else:
                            raise error
                
                # 添加错误处理回调
                self.llm_agent_instance.callbacks.append(ErrorCallbackHandler())
                
            except Exception as e:
                # 检查是否需要回退到Azure
                if "Fallback to Azure" in str(e) or should_fallback_to_azure(e):
                    print(f"初始化或测试OpenAI时出错，使用Azure OpenAI作为备用...")
                    try:
                        # 更新全局LLM类型
                        GlobalLLMConfig.set_current_llm_type("azure-gpt4")
                        self.llm_agent_instance = AzureChatOpenAI(
                            api_key=SecretStr(config.AZURE_OPENAI_API_KEY),
                            azure_endpoint=config.AZURE_OPENAI_API_BASE,
                            azure_deployment=config.AZURE_OPENAI_MODEL_NAME,
                            api_version=config.AZURE_OPENAI_API_VERSION,
                            callbacks=[TokenCallbackHandler(self._token_callback)]
                        )
                        print(f"成功初始化Azure OpenAI作为备用")
                    except Exception as azure_error:
                        print(f"Azure OpenAI初始化也失败: {str(azure_error)}")
                        raise ValueError(f"无法初始化任何LLM服务。OpenAI错误: {str(e)}，Azure错误: {str(azure_error)}")
                else:
                    raise e
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

    # 获取当前使用的LLM类型
    def get_current_llm_type(self) -> str:
        """获取当前使用的LLM类型"""
        return GlobalLLMConfig.get_current_llm_type()
    
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
                # 返回字段识别结果
                yield {"step": "identify_column", "message": "字段识别成功"}
                yield parsed_result
            except json.JSONDecodeError as json_err:
                # If result is not valid JSON, try to extract JSON part
                import re
                
                # Try to find anything that looks like a JSON object
                json_pattern = r'({[\s\S]*?})'
                json_matches = re.findall(json_pattern, result, re.DOTALL)
                
                for potential_json in json_matches:
                    try:
                        parsed_result = json.loads(potential_json)
                        # 返回字段识别结果
                        yield {"step": "identify_column", "message": "字段识别成功"}
                        yield parsed_result
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
                        # 返回字段识别结果
                        yield {"step": "identify_column", "message": "字段识别成功"}
                        yield parsed_result
                    except json.JSONDecodeError:
                        pass
                
                # If all attempts failed, return empty result
                yield {}
                
        except Exception as e:
            import traceback
            yield {}
