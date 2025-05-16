from typing import Optional, Dict, Any
import json
from openai import OpenAI
from utils.singleton import singleton
import os
import sys
from pathlib import Path
from langchain_openai import ChatOpenAI
import config
from tools.token_counter import TokenCounter

ROOT_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = os.path.join(ROOT_DIR, '.env')

class BaseLLM:
    async def generate(self, prompt: str) -> str:
        pass

class OpenAIGPT41(BaseLLM):
    def __init__(self, api_key: str, project_id: str):
        self.client = OpenAI(
            api_key=api_key,
        )
        self.model = "gpt-4.1"
        self.project_id = project_id  # Store project_id as an instance variable
        self.token_counter = TokenCounter()

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
            
            return content if content is not None else "Empty Response Error"
        except Exception as e:
            import traceback
            error_message = str(e)
            print(f"OpenAI GPT-4.1 Error: {error_message}")
            print(traceback.format_exc())
            return f"LLM request error: {error_message}"

@singleton
class LLMService:
    def __init__(self):        
        self.llm_instance: Optional[BaseLLM] = None
        self.llm_agent_instance = None
        self.token_counter = TokenCounter()

    def init_llm(self, **kwargs) -> BaseLLM:
        # If already initialized, return directly
        if self.llm_instance is not None:
            return self.llm_instance
        
        api_key = kwargs.get("api_key", config.OPENAI_API_KEY)
        project_id = kwargs.get("project_id", config.OPENAI_PROJECT_ID)
        if not api_key:
            raise ValueError("OPENAI_API_KEY not configured")
        if not project_id:
            raise ValueError("OPENAI_PROJECT_ID not configured")
        
        self.llm_instance = OpenAIGPT41(
            api_key=api_key,
            project_id=project_id
        )

        return self.llm_instance

    def get_llm(self) -> BaseLLM:
        if not self.llm_instance:
            self.init_llm()
            
        if not self.llm_instance:
            raise RuntimeError("LLM instance not initialized")
            
        return self.llm_instance
        
    def force_reset_llm(self):
        """Force reset LLM instance, used for testing"""
        self.llm_instance = None
        return self.init_llm()

    def init_agent_llm(self, model_name):
        from langchain_openai import ChatOpenAI
        
        # Extract the actual model name without the prefix
        actual_model = model_name.replace("openai-", "")
        
        # For OpenAI models
        api_key = config.OPENAI_API_KEY
        
        self.llm_agent_instance = ChatOpenAI(
            model=actual_model,
            api_key=api_key,
            temperature=0
        )

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
                "model": config.OPENAI_MODEL_NAME,
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
        return "openai-gpt41"
    
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
