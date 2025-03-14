from typing import List, Dict, Any
from utils.singleton import singleton
from service.llm_service import LLMService
import sys
import os

# Add project root to Python path to ensure Retriever can be imported
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import retrievers from new structure with absolute imports
from Retriever.base_retriever import BaseRetriever
from Retriever.opensearch_retriever import OpenSearchRetriever
from Retriever.postgresql_retriever import PostgreSQLRetriever
from Retriever.neo4j_retriever import Neo4jRetriever

@singleton
class RAGService:
    def __init__(self):
        # Initialize LLM service
        self.llm_service = LLMService()
        # 初始化 Azure GPT-4 LLM
        self.llm_service.init_llm("azure-gpt4")
        # Initialize retrievers will be done in retrieve method
        pass
    
    async def _multi_source_retrieve(self, query: str) -> List[Dict[str, Any]]:
        # Initialize retrievers
        retrievers = {
            'opensearch': OpenSearchRetriever(),
            'postgresql': PostgreSQLRetriever(),
            'neo4j': Neo4jRetriever()
        }
        
        results = []
        for source, retriever in retrievers.items():
            try:
                source_results = await retriever.retrieve(query)
                for result in source_results:
                    result['source'] = source  # Add source identifier
                results.extend(source_results)
            except Exception as e:
                import traceback
                print(f"Error retrieving from {source}: {str(e)}")
                print(f"Detailed error: {traceback.format_exc()}")
        return results

    async def _rerank(self, results: List[Dict[str, Any]], query: str) -> List[Dict[str, Any]]:
        # Sort results by score field in descending order
        ranked_results = sorted(results, key=lambda x: x['score'], reverse=True)
        return ranked_results

    async def _process_with_llm(self, query: str, context: List[str]) -> str:
        # Construct prompt
        prompt = f"""
        Please answer the question based on the following information:
        
        Question: {query}
        
        Context:
        {' '.join(context)}
        
        Please provide a detailed and accurate answer. If the context is insufficient to answer the question, please indicate so.
        """
        
        # Call LLM service
        try:
            llm = self.llm_service.get_llm()
            response = await llm.generate(prompt)
            return response
        except Exception as e:
            import traceback
            print(f"LLM processing error: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            return f"Error processing query: {str(e)}"

    async def retrieve(self, query: str) -> List[str]:
        # 1. Get results from multiple data sources
        results = await self._multi_source_retrieve(query)
        
        # 2. Rerank the results
        ranked_results = await self._rerank(results, query)
        
        # 3. Get content from all results
        context = [result['content'] for result in ranked_results]

        return context