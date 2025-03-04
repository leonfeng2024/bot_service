from langchain_community.utilities import SQLDatabase
from typing import List, Dict, Any
from utils.singleton import singleton
from abc import ABC, abstractmethod
from langchain_huggingface import HuggingFaceEmbeddings
from service.llm_service import LLMService
from langchain_community.agent_toolkits import SQLDatabaseToolkit, create_sql_agent
from langchain.agents import AgentType
import config
import psycopg2
import re


class BaseRetriever(ABC):
    @abstractmethod
    async def retrieve(self, query: str) -> List[Dict[str, Any]]:
        pass


class OpenSearchRetriever(BaseRetriever):
    async def retrieve(self, query: str) -> List[Dict[str, Any]]:
        # 模拟OpenSearch检索结果
        results = [
            {"content": "OpenSearch是一个分布式搜索引擎，支持全文检索和实时分析。", "score": 0.95},
            # {"content": "Python的异步编程使用async/await语法，能够高效处理I/O密集型任务。", "score": 0.88},
            # {"content": "FastAPI框架基于Python 3.6+的类型提示构建，具有极高的性能。", "score": 0.82},
            # {"content": "PostgreSQL数据库支持JSON数据类型，适合存储半结构化数据。", "score": 0.75},
            # {"content": "Docker容器化技术简化了应用程序的部署和扩展过程。", "score": 0.68}
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
                # Create the SQL toolkit
                self.toolkit = SQLDatabaseToolkit(db=self.db, llm=self.llm)
                # Create the SQL agent
                self.agent = create_sql_agent(
                    llm=self.llm,
                    toolkit=self.toolkit,
                    agent_type=AgentType.OPENAI_FUNCTIONS,
                    verbose=True
                )

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
        # 模拟Neo4j检索结果
        results = [
            {"content": "Neo4j是一个高性能的图数据库，专门用于处理关联数据。", "score": 0.98},
            # {"content": "MongoDB文档数据库适合处理大规模非结构化数据。", "score": 0.89},
            # {"content": "React框架采用虚拟DOM技术提升渲染性能。", "score": 0.83},
            # {"content": "Go语言以其出色的并发处理能力而闻名。", "score": 0.76},
            # {"content": "Elasticsearch提供强大的全文搜索和分析能力。", "score": 0.70}
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
        """从多个数据源获取检索结果

        Args:
            query: 用户的查询字符串

        Returns:
            List[Dict[str, Any]]: 包含所有数据源检索结果的列表
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
                    result['source'] = source  # 添加来源标记
                results.extend(source_results)
            except Exception as e:
                import traceback
                print(f"Error retrieving from {source}: {str(e)}")
                print(f"Detailed error: {traceback.format_exc()}")
        return results

    async def _rerank(self, results: List[Dict[str, Any]], query: str) -> List[Dict[str, Any]]:
        """对检索结果进行重排序

        Args:
            results: 原始检索结果列表
            query: 用户查询字符串

        Returns:
            List[Dict[str, Any]]: 重排序后的结果列表
        """
        # 根据score字段对结果进行降序排序
        ranked_results = sorted(results, key=lambda x: x['score'], reverse=True)
        return ranked_results

    async def _process_with_llm(self, query: str, context: List[str]) -> str:
        """使用LLM处理查询和上下文

        Args:
            query: 用户的查询字符串
            context: 检索到的上下文信息

        Returns:
            str: LLM生成的回答
        """
        # 构建提示词
        prompt = f"""
        基于以下信息回答问题:
        
        问题: {query}
        
        上下文信息:
        {' '.join(context)}
        
        请提供详细、准确的回答。如果上下文信息不足以回答问题，请说明。
        """
        
        # 调用LLM服务
        try:
            llm = self.llm_service.get_llm()
            response = await llm.generate(prompt)
            return response
        except Exception as e:
            import traceback
            print(f"LLM处理错误: {str(e)}")
            print(f"详细错误: {traceback.format_exc()}")
            return f"处理查询时出错: {str(e)}"

    async def retrieve(self, query: str) -> List[str]:
        """主检索方法

        Args:
            query: 用户的查询字符串

        Returns:
            List[str]: 返回最相关的文档列表
        """
        # 1. 从多个数据源获取结果
        results = await self._multi_source_retrieve(query)
        
        # 2. 对结果进行重排序
        ranked_results = await self._rerank(results, query)
        
        # 3. 获取所有结果的内容
        context = [result['content'] for result in ranked_results]
        
        # 4. 检查是否需要LLM处理
        needs_llm = False
        query_lower = query.lower()
        llm_keywords = ['解释', '比较', '区别', '分析', '为什么', '如何', 'explain', 'compare', 'difference', 'analyze', 'why', 'how']
        
        for keyword in llm_keywords:
            if keyword in query_lower:
                needs_llm = True
                break
        
        # 5. 如果需要LLM处理，则调用LLM
        if needs_llm:
            llm_response = await self._process_with_llm(query, context)
            return [llm_response]
        
        # 6. 否则直接返回检索结果
        return context