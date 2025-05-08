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

# Global LLM model configuration
class GlobalLLMConfig:
    DEFAULT_LLM_TYPE = "openai-gpt41"  # azure-gpt4  openai-gpt41 default to use OpenAI GPT-4.1
    _current_llm_type = DEFAULT_LLM_TYPE
    _is_initialized = False
    _fallback_attempted = False
    
    @classmethod
    def get_current_llm_type(cls):
        if not cls._is_initialized:
            # Try to pre-check OpenAI availability
            cls._check_openai_availability()
            cls._is_initialized = True
        return cls._current_llm_type
    
    @classmethod
    def set_current_llm_type(cls, llm_type: str):
        previous = cls._current_llm_type
        cls._current_llm_type = llm_type
        cls._is_initialized = True
        if previous != llm_type:
            print(f"Global LLM type changed from {previous} to {llm_type}")
    
    @classmethod
    def reset(cls):
        cls._current_llm_type = cls.DEFAULT_LLM_TYPE
        cls._is_initialized = False
        cls._fallback_attempted = False
        print(f"Global LLM type reset to default: {cls.DEFAULT_LLM_TYPE}")
    
    @classmethod
    def _check_openai_availability(cls):
        """Check if OpenAI is available, switch to Azure if not"""
        if cls._fallback_attempted:
            return  # Avoid repeated checks
        
        try:
            # Use native SDK to test connection
            api_key = config.OPENAI_API_KEY
            project_id = config.OPENAI_PROJECT_ID
            test_client = OpenAI(
                api_key=api_key,
            )
            # Simple test request (without headers parameter)
            test_client.models.list()
            # If successful, continue using OpenAI
            print("OpenAI API availability check successful, using OpenAI GPT-4.1")
            cls._current_llm_type = "openai-gpt41"
        except Exception as e:
            error_str = str(e).lower()
            # Check if should switch to Azure
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
                print(f"OpenAI API not available: {str(e)}")
                print("Automatically switching to Azure OpenAI")
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
            
            # Calculate and log token usage
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
            # Ensure response is not empty, return error message if empty
            content = completion.choices[0].message.content
            
            # Calculate and log token usage
            self.token_counter.log_tokens(self.model, prompt, content, source="azure-gpt4")
            
            return content if content is not None else "Empty Response Error"
        except Exception as e:
            import traceback
            # If Azure errors, log but don't switch global model
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
        # Add backup LLM
        self.fallback_llm = None
        # Flag to track if backup LLM has been attempted
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
            # Ensure response is not empty, return error message if empty
            content = completion.choices[0].message.content
            
            # Calculate and log token usage
            self.token_counter.log_tokens(self.model, prompt, content, source="openai-gpt41")
            
            # Reset backup attempt flag
            self.fallback_attempted = False
            
            return content if content is not None else "Empty Response Error"
        except Exception as e:
            import traceback
            error_message = str(e)
            
            # If backup LLM has been attempted, avoid infinite recursion
            if self.fallback_attempted:
                print(f"Already attempted backup LLM, avoiding infinite recursion")
                return f"All LLM services failed: {error_message}"
                
            # Check various possible OpenAI error cases
            if (
                "403" in error_message or  # Permission error
                "401" in error_message or  # Authentication error
                "429" in error_message or  # Rate limit
                "500" in error_message or  # Server error
                "502" in error_message or  # Gateway error
                "503" in error_message or  # Service unavailable
                "504" in error_message or  # Gateway timeout
                "unsupported_country_region_territory" in error_message or  # Region restriction
                "not_found" in error_message or  # Resource not found
                "invalid_api_key" in error_message or  # Invalid API key
                "model not found" in error_message.lower() or  # Model not found
                "timeout" in error_message.lower()  # Connection timeout
            ):
                error_type = ""
                if "403" in error_message and "unsupported_country_region_territory" in error_message:
                    error_type = "Region restriction error"
                elif "403" in error_message:
                    error_type = "Permission error"
                elif "401" in error_message:
                    error_type = "Authentication error"
                elif "429" in error_message:
                    error_type = "Rate limit"
                elif "timeout" in error_message.lower():
                    error_type = "Connection timeout"
                elif "model not found" in error_message.lower():
                    error_type = "Model not found"
                else:
                    error_type = "Server error"
                
                print(f"{error_type}: OpenAI API not available. Falling back to Azure OpenAI.")
                print(f"Original error: {error_message}")
                
                # Set global LLM type to Azure
                GlobalLLMConfig.set_current_llm_type("azure-gpt4")
                
                # Mark backup LLM as attempted
                self.fallback_attempted = True
                
                # Try using Azure OpenAI as backup
                try:
                    if self.fallback_llm is None:
                        # Initialize Azure backup LLM
                        self.fallback_llm = AzureGPT4(
                            api_key=config.AZURE_OPENAI_API_KEY,
                            api_base=config.AZURE_OPENAI_API_BASE,
                            api_version=config.AZURE_OPENAI_API_VERSION
                        )
                    
                    # Use backup LLM
                    print(f"Using Azure OpenAI as backup...")
                    fallback_response = await self.fallback_llm.generate(prompt)
                    return fallback_response
                except Exception as fallback_error:
                    print(f"Backup LLM also failed: {str(fallback_error)}")
                    return f"OpenAI API not available ({error_type}), backup service also failed: {str(fallback_error)}. Please check your network environment or configuration."
            
            print(f"OpenAI GPT-4.1 Error: {error_message}")
            print(traceback.format_exc())
            return f"LLM request error: {error_message}"


@singleton
class LLMService:
    def __init__(self):        
        self.llm_instance: Optional[BaseLLM] = None
        self.llm_agent_instance = None
        self.token_counter = TokenCounter()
        self.last_init_type = None

    def init_llm(self, llm_type: Optional[str] = None, **kwargs) -> BaseLLM:
        # If llm_type not specified, use global configuration
        if llm_type is None:
            llm_type = GlobalLLMConfig.get_current_llm_type()
            print(f"Using global LLM type: {llm_type}")
        
        # If already initialized with same type, return directly
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
                # If OpenAI initialization fails, automatically switch to Azure
                print(f"OpenAI initialization failed, automatically switching to Azure: {str(e)}")
                GlobalLLMConfig.set_current_llm_type("azure-gpt4")
                return self.init_llm("azure-gpt4", **kwargs)
        else:
            raise ValueError(f"Unsupported LLM type: {llm_type}")

        return self.llm_instance

    def get_llm(self) -> BaseLLM:
        # Check current global configuration before each call, reinitialize if different
        current_type = GlobalLLMConfig.get_current_llm_type()
        if self.last_init_type != current_type:
            print(f"Detected global LLM type change: {self.last_init_type} -> {current_type}")
            self.init_llm(current_type)
        
        if not self.llm_instance:
            self.init_llm()
            
        if not self.llm_instance:
            raise RuntimeError("LLM instance not initialized")
            
        return self.llm_instance
        
    def force_reset_llm(self, llm_type: Optional[str] = None):
        """Force reset LLM instance, used for testing or manual switching"""
        self.llm_instance = None
        self.last_init_type = None
        if llm_type:
            GlobalLLMConfig.set_current_llm_type(llm_type)
        return self.init_llm()

    def init_agent_llm(self, llm_type: Optional[str] = None):
        # If llm_type not specified, use global configuration
        if llm_type is None:
            llm_type = GlobalLLMConfig.get_current_llm_type()
            print(f"Using global LLM type to initialize Agent: {llm_type}")
            
        # Create BaseCallbackHandler instance
        from langchain_core.callbacks import BaseCallbackHandler
        class TokenCallbackHandler(BaseCallbackHandler):
            def __init__(self, callback_func):
                super().__init__()
                self.callback_func = callback_func
            
            def on_llm_end(self, response, **kwargs):
                """Properly handle langchain's LLM end callback"""
                if hasattr(response, 'llm_output') and 'token_usage' in response.llm_output:
                    self.callback_func(token_usage=response.llm_output['token_usage'])
                else:
                    self.callback_func(**kwargs)
            
        if llm_type == "azure-gpt4":
            api_key = "" if config.AZURE_OPENAI_API_KEY is None else config.AZURE_OPENAI_API_KEY
            if not api_key:
                raise ValueError("AZURE_OPENAI_API_KEY not configured")
            
            # Initialize AzureChatOpenAI
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
            
            # Define error handling function
            def should_fallback_to_azure(error_message):
                """Determine if should fall back to Azure OpenAI"""
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
            
            # Initialize ChatOpenAI 
            try:
                # First try to test OpenAI connection
                try:
                    # Use native SDK to test connection
                    test_client = OpenAI(
                        api_key=api_key,
                    )
                    # Simple test request - this will trigger any connection or auth issues (without headers parameter)
                    test_client.models.list()
                    print("OpenAI connection test successful, continuing with GPT-4.1")
                except Exception as test_error:
                    if should_fallback_to_azure(test_error):
                        print(f"OpenAI connection test failed: {str(test_error)}")
                        print("Directly switching to Azure OpenAI as backup")
                        # Update global LLM type
                        GlobalLLMConfig.set_current_llm_type("azure-gpt4")
                        # Recursively call self with Azure type
                        return self.init_agent_llm("azure-gpt4")
                    else:
                        # Other errors might just be test method issues, still try initialization
                        print(f"OpenAI connection test had non-fatal error: {str(test_error)}")
                
                # Initialize LangChain ChatOpenAI
                self.llm_agent_instance = ChatOpenAI(
                    model=config.OPENAI_MODEL_NAME,
                    openai_api_key=config.OPENAI_API_KEY,
                    callbacks=[TokenCallbackHandler(self._token_callback)]
                )
                
                # Register error handling callback
                class ErrorCallbackHandler(BaseCallbackHandler):
                    def on_llm_error(self, error, **kwargs):
                        if should_fallback_to_azure(error):
                            print(f"LangChain OpenAI error, falling back to Azure: {str(error)}")
                            # Update global LLM type
                            GlobalLLMConfig.set_current_llm_type("azure-gpt4")
                            raise ValueError(f"Fallback to Azure: {str(error)}")
                        else:
                            raise error
                
                # Add error handling callback
                self.llm_agent_instance.callbacks.append(ErrorCallbackHandler())
                
            except Exception as e:
                # Check if should fall back to Azure
                if "Fallback to Azure" in str(e) or should_fallback_to_azure(e):
                    print(f"Error initializing or testing OpenAI, using Azure OpenAI as backup...")
                    try:
                        # Update global LLM type
                        GlobalLLMConfig.set_current_llm_type("azure-gpt4")
                        self.llm_agent_instance = AzureChatOpenAI(
                            api_key=SecretStr(config.AZURE_OPENAI_API_KEY),
                            azure_endpoint=config.AZURE_OPENAI_API_BASE,
                            azure_deployment=config.AZURE_OPENAI_MODEL_NAME,
                            api_version=config.AZURE_OPENAI_API_VERSION,
                            callbacks=[TokenCallbackHandler(self._token_callback)]
                        )
                        print(f"Successfully initialized Azure OpenAI as backup")
                    except Exception as azure_error:
                        print(f"Azure OpenAI initialization also failed: {str(azure_error)}")
                        raise ValueError(f"Cannot initialize any LLM service. OpenAI error: {str(e)}, Azure error: {str(azure_error)}")
                else:
                    raise e
        else:
            raise ValueError(f"Unsupported LLM type: {llm_type}")

        return self.llm_agent_instance
    
    def _token_callback(self, **kwargs):
        """Callback function to record token usage from langchain calls"""
        # Check if contains token usage information
        if "token_usage" in kwargs:
            usage = kwargs["token_usage"]
            input_tokens = usage.get("prompt_tokens", 0)
            output_tokens = usage.get("completion_tokens", 0)
            
            # Log token usage
            print(f"[Token Usage] langchain - Input: {input_tokens} tokens, Output: {output_tokens} tokens")
            
            # Update total counts
            self.token_counter.total_input_tokens += input_tokens
            self.token_counter.total_output_tokens += output_tokens
            
            # Record this call
            call_record = {
                "source": "langchain",
                "model": config.AZURE_OPENAI_MODEL_NAME,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens
            }
            self.token_counter.calls_history.append(call_record)

    def get_token_usage(self) -> Dict[str, Any]:
        """Get token usage statistics"""
        return self.token_counter.get_total_usage()
    
    def get_formatted_token_usage(self) -> str:
        """Get formatted token usage statistics"""
        return self.token_counter.get_formatted_usage()

    def get_current_llm_type(self) -> str:
        """Get current LLM type in use"""
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
                # Return field identification result
                yield {"step": "identify_column", "message": "Field identification successful"}
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
                        # Return field identification result
                        yield {"step": "identify_column", "message": "Field identification successful"}
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
                        # Return field identification result
                        yield {"step": "identify_column", "message": "Field identification successful"}
                        yield parsed_result
                    except json.JSONDecodeError:
                        pass
                
                # If all attempts failed, return empty result
                yield {}
                
        except Exception as e:
            import traceback
            yield {}
