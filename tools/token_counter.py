from typing import Dict, Any, Optional, Tuple, List
import tiktoken
import logging

logger = logging.getLogger(__name__)

class TokenCounter:
    """Token计数器，用于计算LLM调用的token消耗"""
    
    def __init__(self):
        # 初始化不同模型的编码器
        self.encoders = {}
        # 记录总token消耗
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        # 记录每次调用的token消耗
        self.calls_history = []
    
    def _get_encoder(self, model_name: str):
        """获取或创建指定模型的编码器"""
        if model_name not in self.encoders:
            try:
                # 根据模型名称获取合适的编码器
                if "gpt-4" in model_name.lower():
                    self.encoders[model_name] = tiktoken.encoding_for_model("gpt-4")
                elif "gpt-3.5" in model_name.lower():
                    self.encoders[model_name] = tiktoken.encoding_for_model("gpt-3.5-turbo")
                elif "claude" in model_name.lower():
                    # Claude使用cl100k_base编码器
                    self.encoders[model_name] = tiktoken.get_encoding("cl100k_base")
                else:
                    # 默认使用cl100k_base编码器
                    self.encoders[model_name] = tiktoken.get_encoding("cl100k_base")
                    
                logger.info(f"Created encoder for model: {model_name}")
            except Exception as e:
                logger.error(f"Error creating encoder for model {model_name}: {str(e)}")
                # 使用默认编码器
                self.encoders[model_name] = tiktoken.get_encoding("cl100k_base")
        
        return self.encoders[model_name]
    
    def count_tokens(self, text: str, model_name: str) -> int:
        """计算文本的token数量"""
        if not text:
            return 0
            
        try:
            encoder = self._get_encoder(model_name)
            tokens = encoder.encode(text)
            return len(tokens)
        except Exception as e:
            logger.error(f"Error counting tokens: {str(e)}")
            # 使用简单的估算方法作为备选
            return len(text) // 4  # 粗略估计每个token约4个字符
    
    def log_tokens(self, model_name: str, input_text: str, output_text: str, 
                   source: str = "general") -> Tuple[int, int]:
        """记录一次LLM调用的token消耗"""
        input_tokens = self.count_tokens(input_text, model_name)
        output_tokens = self.count_tokens(output_text, model_name)
        
        # 更新总计数
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens
        
        # 记录本次调用
        call_record = {
            "source": source,
            "model": model_name,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens
        }
        self.calls_history.append(call_record)
        
        # 打印到控制台
        print(f"[Token Usage] {source} - Input: {input_tokens} tokens, Output: {output_tokens} tokens")
        
        return input_tokens, output_tokens
    
    def get_total_usage(self) -> Dict[str, int]:
        """获取总token使用量"""
        return {
            "input_tokens": self.total_input_tokens,
            "output_tokens": self.total_output_tokens,
            "total_tokens": self.total_input_tokens + self.total_output_tokens
        }
    
    def get_usage_by_source(self) -> Dict[str, Dict[str, int]]:
        """按来源获取token使用量"""
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
        """重置计数器"""
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.calls_history = []
    
    def format_token_count(self, count: int) -> str:
        """格式化token数量，大于1000的显示为k"""
        if count >= 1000:
            return f"{count/1000:.2f}k"
        return str(count)
    
    def get_formatted_usage(self) -> str:
        """获取格式化的使用量统计信息"""
        usage = self.get_total_usage()
        return f"input token: {self.format_token_count(usage['input_tokens'])}\noutput token: {self.format_token_count(usage['output_tokens'])}"