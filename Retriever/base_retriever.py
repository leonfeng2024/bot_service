from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseRetriever(ABC):
    """基础抽象检索器类，所有检索器都应该继承这个类"""
    
    def _print_prompt(self, prompt: str) -> None:
        """
        打印prompt到控制台
        
        Args:
            prompt: 要打印的prompt字符串
        """
        print(f"\n=== Retriever Prompt ===\n{prompt}\n=====================\n")

    @abstractmethod
    async def retrieve(self, query: str) -> List[Dict[str, Any]]:
        """
        检索相关文档
        
        Args:
            query: 用户查询字符串
            
        Returns:
            List[Dict[str, Any]]: 返回检索结果列表，每个结果包含内容和相关性分数
        """
        pass