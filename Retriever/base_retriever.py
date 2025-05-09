from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseRetriever(ABC):
    """Base abstract retriever class, all retrievers should inherit from this class"""
    
    def _print_prompt(self, prompt: str) -> None:
        """
        Print prompt to console
        
        Args:
            prompt: The prompt string to print
        """
        print(f"\n=== Retriever Prompt ===\n{prompt}\n=====================\n")

    @abstractmethod
    async def retrieve(self, query: str) -> List[Dict[str, Any]]:
        """
        Retrieve relevant documents
        
        Args:
            query: User query string
            
        Returns:
            List[Dict[str, Any]]: List of retrieval results, each containing content and relevance score
        """
        pass