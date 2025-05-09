from typing import Dict, Any, Optional, Tuple, List
import tiktoken
import logging

logger = logging.getLogger(__name__)

class TokenCounter:
    """Token counter for calculating LLM call token consumption"""
    
    def __init__(self):
        # Initialize encoders for different models
        self.encoders = {}
        # Record total token consumption
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        # Record token consumption for each call
        self.calls_history = []
    
    def _get_encoder(self, model_name: str):
        """Get or create encoder for specified model"""
        if model_name not in self.encoders:
            try:
                # Get appropriate encoder based on model name
                if "gpt-4" in model_name.lower():
                    self.encoders[model_name] = tiktoken.encoding_for_model("gpt-4")
                elif "gpt-3.5" in model_name.lower():
                    self.encoders[model_name] = tiktoken.encoding_for_model("gpt-3.5-turbo")
                elif "claude" in model_name.lower():
                    # Claude uses cl100k_base encoder
                    self.encoders[model_name] = tiktoken.get_encoding("cl100k_base")
                else:
                    # Use cl100k_base encoder as default
                    self.encoders[model_name] = tiktoken.get_encoding("cl100k_base")
                    
                logger.info(f"Created encoder for model: {model_name}")
            except Exception as e:
                logger.error(f"Error creating encoder for model {model_name}: {str(e)}")
                # Use default encoder
                self.encoders[model_name] = tiktoken.get_encoding("cl100k_base")
        
        return self.encoders[model_name]
    
    def count_tokens(self, text: str, model_name: str) -> int:
        """Calculate number of tokens in text"""
        if not text:
            return 0
            
        try:
            encoder = self._get_encoder(model_name)
            tokens = encoder.encode(text)
            return len(tokens)
        except Exception as e:
            logger.error(f"Error counting tokens: {str(e)}")
            # Use simple estimation method as fallback
            return len(text) // 4  # Rough estimate: 1 token ≈ 4 characters
    
    def log_tokens(self, model_name: str, input_text: str, output_text: str, 
                   source: str = "general") -> Tuple[int, int]:
        """Record token consumption for one LLM call"""
        input_tokens = self.count_tokens(input_text, model_name)
        output_tokens = self.count_tokens(output_text, model_name)
        
        # Update total count
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens
        
        # Record this call
        call_record = {
            "source": source,
            "model": model_name,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens
        }
        self.calls_history.append(call_record)
        
        # Print to console
        print(f"[Token Usage] {source} - Input: {input_tokens} tokens, Output: {output_tokens} tokens")
        
        return input_tokens, output_tokens
    
    def get_total_usage(self) -> Dict[str, int]:
        """Get total token usage"""
        return {
            "input_tokens": self.total_input_tokens,
            "output_tokens": self.total_output_tokens,
            "total_tokens": self.total_input_tokens + self.total_output_tokens
        }
    
    def get_usage_by_source(self) -> Dict[str, Dict[str, int]]:
        """Get token usage by source"""
        usage_by_source = {}
        
        for call in self.calls_history:
            source = call["source"]
            if source not in usage_by_source:
                usage_by_source[source] = {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "total_tokens": 0
                }
            
            usage_by_source[source]["input_tokens"] += call["input_tokens"]
            usage_by_source[source]["output_tokens"] += call["output_tokens"]
            usage_by_source[source]["total_tokens"] += call["input_tokens"] + call["output_tokens"]
        
        return usage_by_source
    
    def reset(self):
        """Reset counter"""
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.calls_history = []
    
    def format_token_count(self, count: int) -> str:
        """Format token count, display as k for numbers over 1000"""
        if count >= 1000:
            return f"{count/1000:.2f}k"
        return str(count)
    
    def get_formatted_usage(self) -> str:
        """Get formatted usage statistics"""
        usage = self.get_total_usage()
        return f"input token: {self.format_token_count(usage['input_tokens'])}\noutput token: {self.format_token_count(usage['output_tokens'])}"