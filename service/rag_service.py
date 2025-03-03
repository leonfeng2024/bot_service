from langchain_community.utilities import SQLDatabase
from typing import List, Dict, Any
from utils.singleton import singleton
from abc import ABC, abstractmethod
from langchain_community.vectorstores import Chroma
from langchain_huggingface import HuggingFaceEmbeddings
from service.llm_service import LLMService
from langchain_community.agent_toolkits import SQLDatabaseToolkit, create_sql_agent
from langchain.agents import AgentType


CHROMA_PATH = r"C:\Users\pinjing.wu\OneDrive - Accenture\Project\Takeda\TakedaProject\GenAI\bot_service\chroma_db"

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
    async def retrieve(self, query: str) -> List[Dict[str, Any]]:
        db_uri = "postgresql+psycopg2://postgres:Automation2025@localhost:5432/test_rag"
        db = SQLDatabase.from_uri(db_uri)
        llm = LLMService().init_agent_llm("azure-gpt4")
        toolkit = SQLDatabaseToolkit(db=db, llm=llm)
        agent_executor = create_sql_agent(
            llm=llm,
            toolkit=toolkit,
            verbose=True,
            agent_type=AgentType.OPENAI_FUNCTIONS,
            top_k=10000
        )

        result = agent_executor.invoke({"input": query})

        return [{"content": result['output'], "score": 0.99}]


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


class ChromaRetriever(BaseRetriever):
    async def retrieve(self, query: str) -> List[Dict[str, Any]]:
        embd = HuggingFaceEmbeddings(model_name="BAAI/bge-m3")
        vectorstore = Chroma(persist_directory=CHROMA_PATH, embedding_function=embd)
        search_results = vectorstore.similarity_search(query, k=5)

        results = []
        for i, doc in enumerate(search_results):
            results.append({"content": doc.page_content, "score": 0.9})

        print("chroma: ", str(results))

        return results


@singleton
class RAGService:
    def __init__(self):
        # 初始化检索器
        self.retrievers = {
            'chroma': ChromaRetriever(),
            'opensearch': OpenSearchRetriever(),
            'postgresql': PostgreSQLRetriever(),
            'neo4j': Neo4jRetriever()
        }
    
    async def _multi_source_retrieve(self, query: str) -> List[Dict[str, Any]]:
        """从多个数据源获取检索结果

        Args:
            query: 用户的查询字符串

        Returns:
            List[Dict[str, Any]]: 包含所有数据源检索结果的列表
        """
        results = []
        for source, retriever in self.retrievers.items():
            try:
                source_results = await retriever.retrieve(query)
                for result in source_results:
                    result['source'] = source  # 添加来源标记
                results.extend(source_results)
            except Exception as e:
                print(f"Error retrieving from {source}: {str(e)}")
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
        print(ranked_results)
        # 3. 返回所有结果的内容
        return [result['content'] for result in ranked_results]