from langchain_community.utilities import SQLDatabase
from typing import List, Dict, Any
from utils.singleton import singleton
from abc import ABC, abstractmethod
from langchain_community.embeddings import HuggingFaceEmbeddings
from service.llm_service import LLMService
from langchain_community.agent_toolkits import SQLDatabaseToolkit, create_sql_agent
from langchain.agents import AgentType
import config
import re


class BaseRetriever(ABC):
    @abstractmethod
    async def retrieve(self, query: str) -> List[Dict[str, Any]]:
        pass


class OpenSearchRetriever(BaseRetriever):
    async def retrieve(self, query: str) -> List[Dict[str, Any]]:
        # Simulated OpenSearch retrieval results
        results = [
            {"content": "OpenSearch is a distributed search engine that supports full-text search and real-time analytics.", "score": 0.95},
            # {"content": "Python's async programming uses async/await syntax for efficient I/O-intensive task handling.", "score": 0.88},
            # {"content": "FastAPI framework is built on Python 3.6+ type hints and offers extremely high performance.", "score": 0.82},
            # {"content": "PostgreSQL database supports JSON data type, suitable for semi-structured data storage.", "score": 0.75},
            # {"content": "Docker containerization technology simplifies application deployment and scaling.", "score": 0.68}
        ]
        return results


class PostgreSQLRetriever(BaseRetriever):
    def __init__(self):
        # Initialize the LLM service for the agent
        self.llm_service = LLMService()
        # Initialize the LLM for the agent
        self.llm_service.init_agent_llm("azure-gpt4")
        self.llm = self.llm_service.llm_agent_instance
        # Create the SQL database connection string
        self.db_uri = f"postgresql://{config.POSTGRESQL_USER}:{config.POSTGRESQL_PASSWORD}@{config.POSTGRESQL_HOST}:{config.POSTGRESQL_PORT}/{config.POSTGRESQL_DBNAME}"
        # Initialize the database connection
        self.db = None
        self.toolkit = None
        self.agent = None

    async def retrieve(self, query: str) -> List[Dict[str, Any]]:
        try:
            # Initialize the database connection if not already done
            if self.db is None:
                self.db = SQLDatabase.from_uri(
                    self.db_uri,
                    sample_rows_in_table_info=5
                )
                # 确保 self.llm 不为 None
                if self.llm is not None:
                    # Create the SQL toolkit
                    self.toolkit = SQLDatabaseToolkit(db=self.db, llm=self.llm)
                    # Create the SQL agent
                    self.agent = create_sql_agent(
                        llm=self.llm,
                        toolkit=self.toolkit,
                        agent_type=AgentType.OPENAI_FUNCTIONS,
                        verbose=True
                    )
                else:
                    raise ValueError("LLM instance is not initialized properly")

            # Process the query with the agent
            # Translate the query to make it more SQL-friendly if needed
            sql_query = f"Based on the database schema, answer this question about the database: {query}"
            
            # Run the agent
            result = await self.agent.ainvoke({"input": sql_query})
            
            # Extract the agent's response
            agent_response = result.get("output", "No response from SQL agent")
            
            # Return the result in the expected format
            return [{"content": agent_response, "score": 0.99}]
            
        except Exception as e:
            import traceback
            print(f"PostgreSQL retriever error: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            # Return an error message instead of raising an exception
            return [{"content": f"Error querying PostgreSQL: {str(e)}", "score": 0.0}]


class Neo4jRetriever(BaseRetriever):
    async def retrieve(self, query: str) -> List[Dict[str, Any]]:
        # Simulated Neo4j retrieval results
        results = [
            {"content": "Neo4j is a high-performance graph database specialized in handling connected data.", "score": 0.98},
            # {"content": "MongoDB document database is suitable for handling large-scale unstructured data.", "score": 0.89},
            # {"content": "React framework uses Virtual DOM technology to enhance rendering performance.", "score": 0.83},
            # {"content": "Go language is known for its excellent concurrency handling capabilities.", "score": 0.76},
            # {"content": "Elasticsearch provides powerful full-text search and analytics capabilities.", "score": 0.70}
        ]
        return results


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
        """Retrieve results from multiple data sources

        Args:
            query: User's query string

        Returns:
            List[Dict[str, Any]]: List containing retrieval results from all data sources
        """
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
        """Rerank the retrieval results

        Args:
            results: Original list of retrieval results
            query: User's query string

        Returns:
            List[Dict[str, Any]]: Reranked results list
        """
        # Sort results by score field in descending order
        ranked_results = sorted(results, key=lambda x: x['score'], reverse=True)
        return ranked_results

    async def _process_with_llm(self, query: str, context: List[str]) -> str:
        """Process query and context using LLM

        Args:
            query: User's query string
            context: Retrieved context information

        Returns:
            str: Answer generated by LLM
        """
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
        """Main retrieval method

        Args:
            query: User's query string

        Returns:
            List[str]: Returns list of most relevant documents
        """
        # 1. Get results from multiple data sources
        results = await self._multi_source_retrieve(query)
        
        # 2. Rerank the results
        ranked_results = await self._rerank(results, query)
        
        # 3. Get content from all results
        context = [result['content'] for result in ranked_results]

        return context