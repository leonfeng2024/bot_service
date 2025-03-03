from langchain_community.utilities import SQLDatabase
from typing import List, Dict, Any
from utils.singleton import singleton
from abc import ABC, abstractmethod
from langchain_huggingface import HuggingFaceEmbeddings
from service.llm_service import LLMService
from langchain_community.agent_toolkits import SQLDatabaseToolkit, create_sql_agent
from langchain.agents import AgentType
import config


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
        try:
            import psycopg2
            import re
            
            # Connect to the database using config
            conn = psycopg2.connect(
                dbname=config.POSTGRESQL_DBNAME,
                user=config.POSTGRESQL_USER,
                password=config.POSTGRESQL_PASSWORD,
                host=config.POSTGRESQL_HOST,
                port=config.POSTGRESQL_PORT
            )
            
            # Create a cursor
            cur = conn.cursor()
            
            # Simple query handling based on keywords
            query_lower = query.lower()
            
            # Check for structure query first (more specific)
            if re.search(r'(结构|schema|columns|字段)', query_lower):
                # Try to extract table name
                match = re.search(r'(表|table)\s*[：:]*\s*(\w+)', query_lower)
                
                if match:
                    table_name = match.group(2)
                    
                    cur.execute("""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_name = %s
                        ORDER BY ordinal_position;
                    """, (table_name,))
                    
                    columns = cur.fetchall()
                    if columns:
                        column_info = []
                        for col in columns:
                            nullable = "可为空" if col[2] == "YES" else "非空"
                            column_info.append(f"{col[0]} ({col[1]}, {nullable})")
                        
                        result_content = f"表 {table_name} 的结构:\n" + "\n".join(column_info)
                    else:
                        result_content = f"未找到表 {table_name}"
                else:
                    result_content = "请指定要查询的表名"
            
            # Check for general tables query (more specific than data query for "数据库中有哪些表")
            elif re.search(r'(数据库.*表|database.*tables|所有.*表|all.*tables)', query_lower):
                # Get the list of tables
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name;
                """)
                
                tables = cur.fetchall()
                table_list = [table[0] for table in tables]
                result_content = f"数据库中的表: {', '.join(table_list)}"
            
            # Then check for data query (more specific)
            elif re.search(r'(数据|data|内容|content)', query_lower) and re.search(r'(表|table)\s*[：:]*\s*(\w+)', query_lower):
                # Extract table name
                match = re.search(r'(表|table)\s*[：:]*\s*(\w+)', query_lower)
                
                table_name = match.group(2)
                
                # Check if table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' AND table_name = %s
                    );
                """, (table_name,))
                
                if cur.fetchone()[0]:
                    # Get sample data (first 5 rows)
                    cur.execute(f"""
                        SELECT * FROM {table_name} LIMIT 5;
                    """)
                    
                    rows = cur.fetchall()
                    
                    # Get column names
                    cur.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = %s
                        ORDER BY ordinal_position;
                    """, (table_name,))
                    
                    columns = [col[0] for col in cur.fetchall()]
                    
                    # Format the result
                    result_content = f"表 {table_name} 的示例数据 (最多5行):\n"
                    result_content += "列名: " + ", ".join(columns) + "\n"
                    
                    for row in rows:
                        result_content += str(row) + "\n"
                else:
                    result_content = f"未找到表 {table_name}"
            
            # Then check for general tables query (less specific)
            elif "表" in query_lower or "tables" in query_lower:
                # Get the list of tables
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name;
                """)
                
                tables = cur.fetchall()
                table_list = [table[0] for table in tables]
                result_content = f"数据库中的表: {', '.join(table_list)}"
            
            # Default response
            else:
                # Default response with database summary
                cur.execute("""
                    SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';
                """)
                table_count = cur.fetchone()[0]
                
                result_content = f"PostgreSQL数据库包含 {table_count} 个表。您可以查询特定表的结构或数据。"
            
            # Close the connection
            cur.close()
            conn.close()
            
            return [{"content": result_content, "score": 0.99}]
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