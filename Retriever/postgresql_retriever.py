from typing import List, Dict, Any
import traceback
import json
import asyncpg
from Retriever.base_retriever import BaseRetriever
from service.embedding_service import EmbeddingService
from service.llm_service import LLMService

class PostgreSQLRetriever(BaseRetriever):
    """PostgreSQL检索器，用于从PostgreSQL数据库中检索数据"""
    
    def __init__(self):
        # PostgreSQL connection settings
        self.pg_host = "localhost"
        self.pg_port = 5432
        self.pg_user = "postgres"
        self.pg_password = "postgres"
        self.pg_database = "postgres"
        self.connection_pool = None
        
        # Initialize EmbeddingService for query vectorization
        self.embedding_service = EmbeddingService()
        
        # Initialize LLMService for column identification
        self.llm_service = LLMService()
    
    async def _create_connection_pool(self):
        """Create a connection pool to PostgreSQL"""
        try:
            if self.connection_pool is None:
                self.connection_pool = await asyncpg.create_pool(
                    host=self.pg_host,
                    port=self.pg_port,
                    user=self.pg_user,
                    password=self.pg_password,
                    database=self.pg_database,
                    min_size=1,
                    max_size=10
                )
                print("PostgreSQLRetriever successfully connected to PostgreSQL")
            return self.connection_pool
        except Exception as e:
            print(f"PostgreSQLRetriever error connecting to PostgreSQL: {str(e)}")
            print("Make sure PostgreSQL is running and configuration is correct.")
            return None
    
    async def _search_schema_info(self, term: str) -> List[Dict[str, Any]]:
        """Search for table and column information in PostgreSQL schema"""
        try:
            pool = await self._create_connection_pool()
            if not pool:
                return [{"content": "Failed to connect to PostgreSQL", "score": 0.89}]
            
            # Query to search for tables, columns and their descriptions
            query = """
            SELECT 
                t.table_schema,
                t.table_name,
                c.column_name,
                pg_catalog.obj_description(
                    format('%s.%s', t.table_schema, t.table_name)::regclass::oid, 'pg_class'
                ) as table_description,
                col_description(
                    format('%s.%s', t.table_schema, t.table_name)::regclass::oid, 
                    c.ordinal_position
                ) as column_description
            FROM 
                information_schema.tables t
            JOIN 
                information_schema.columns c 
                ON t.table_schema = c.table_schema AND t.table_name = c.table_name
            WHERE 
                t.table_schema NOT IN ('pg_catalog', 'information_schema')
                AND (
                    t.table_name ILIKE $1 
                    OR c.column_name ILIKE $1
                    OR pg_catalog.obj_description(
                        format('%s.%s', t.table_schema, t.table_name)::regclass::oid, 'pg_class'
                    ) ILIKE $1
                    OR col_description(
                        format('%s.%s', t.table_schema, t.table_name)::regclass::oid, 
                        c.ordinal_position
                    ) ILIKE $1
                )
            ORDER BY 
                t.table_schema, t.table_name, c.ordinal_position
            LIMIT 20;
            """
            
            search_pattern = f"%{term}%"
            
            async with pool.acquire() as conn:
                rows = await conn.fetch(query, search_pattern)
            
            results = []
            current_table = None
            table_info = ""
            
            for row in rows:
                table_id = f"{row['table_schema']}.{row['table_name']}"
                
                # If we have a new table, start a new table info block
                if current_table != table_id:
                    # Add the previous table info to results if it exists
                    if table_info:
                        results.append({"content": table_info, "score": 0.89})
                    
                    # Start new table info
                    current_table = table_id
                    table_info = f"Table '{table_id}':\n"
                    if row['table_description']:
                        table_info += f"Description: {row['table_description']}\n"
                    table_info += "Columns:\n"
                
                # Add column info
                column_info = f"- {row['column_name']}"
                if row['column_description']:
                    column_info += f" ({row['column_description']})"
                table_info += column_info + "\n"
            
            # Add the last table info if it exists
            if table_info:
                results.append({"content": table_info, "score": 0.89})
                
            if not results:
                return [{"content": f"No database objects found matching '{term}'", "score": 0.89}]
                
            return results
            
        except Exception as e:
            print(f"Error searching PostgreSQL for term '{term}': {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            return [{"content": f"Error searching PostgreSQL: {str(e)}", "score": 0.89}]
    
    async def _execute_sample_query(self, table_name: str) -> List[Dict[str, Any]]:
        """Execute a sample query to get a few rows from the table"""
        try:
            pool = await self._create_connection_pool()
            if not pool:
                return [{"content": "Failed to connect to PostgreSQL", "score": 0.89}]
            
            # Extract schema and table
            parts = table_name.split('.')
            if len(parts) == 2:
                schema, table = parts
                full_table_name = f'"{schema}"."{table}"'
            else:
                full_table_name = f'"{table_name}"'
            
            # Get column names
            query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = $1 
            ORDER BY ordinal_position
            LIMIT 10
            """
            
            async with pool.acquire() as conn:
                columns = await conn.fetch(query, parts[-1])
                
                if not columns:
                    return [{"content": f"Table {table_name} not found or has no columns", "score": 0.89}]
                
                # Build a sample query with only the first few columns to avoid excessive data
                column_names = [f'"{col["column_name"]}"' for col in columns[:5]]
                sample_query = f"""
                SELECT {', '.join(column_names)}
                FROM {full_table_name}
                LIMIT 5
                """
                
                try:
                    rows = await conn.fetch(sample_query)
                    
                    # Format the result
                    result = f"Sample data from {table_name}:\n"
                    # Add header
                    header = " | ".join([col["column_name"] for col in columns[:5]])
                    result += header + "\n"
                    result += "-" * len(header) + "\n"
                    
                    # Add rows
                    for row in rows:
                        row_str = " | ".join([str(row[col["column_name"]]) for col in columns[:5]])
                        result += row_str + "\n"
                    
                    return [{"content": result, "score": 0.89}]
                except Exception as e:
                    return [{"content": f"Error querying {table_name}: {str(e)}", "score": 0.89}]
                
        except Exception as e:
            print(f"Error executing sample query for '{table_name}': {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            return [{"content": f"Error executing sample query: {str(e)}", "score": 0.89}]
    
    async def retrieve(self, query: str) -> List[Dict[str, Any]]:
        return [{"content": "employee_details结合了 employees 表和 employee_details 视图", "score": 0.89}] 

    async def retrieve2(self, query: str) -> List[Dict[str, Any]]:
        """
        检索与查询相关的结果
        
        Args:
            query: 用户的查询字符串
            
        Returns:
            List[Dict[str, Any]]: 检索结果列表
        """
        try:
            # 调用LLM分析查询，识别查询的字段名
            print(f"Analyzing query: {query}")
            columns = await self.llm_service.identify_column(query)
            print(f"Identified columns: {json.dumps(columns, ensure_ascii=False)}")
            
            all_results = []
            
            # 如果识别出了字段名，对每个字段进行搜索
            if columns:
                for key, term in columns.items():
                    print(f"Searching for term: {term}")
                    # 先搜索模式信息
                    schema_results = await self._search_schema_info(term)
                    all_results.extend(schema_results)
                    
                    # 如果得到的结果中包含表名，尝试执行样例查询
                    for result in schema_results:
                        content = result.get("content", "")
                        if content.startswith("Table '"):
                            table_name = content.split("'")[1]
                            sample_results = await self._execute_sample_query(table_name)
                            all_results.extend(sample_results)
            else:
                # 如果没有识别出字段名，直接使用原始查询
                print(f"No columns identified, using original query: {query}")
                all_results.extend(await self._search_schema_info(query))
            
            # 如果没有结果，返回未找到的消息
            if not all_results:
                return [{"content": f"No database objects found for query: '{query}'", "score": 0.89}]
                
            return all_results
            
        except Exception as e:
            print(f"PostgreSQL retrieval error: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            return [{"content": f"Error querying PostgreSQL: {str(e)}", "score": 0.89}] 