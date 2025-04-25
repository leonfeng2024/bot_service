import logging
import csv
import os
import asyncio
from typing import List, Dict, Any, Optional
from tools.postgresql_tools import PostgreSQLTools

logger = logging.getLogger(__name__)

class PostgresService:
    def __init__(self):
        self.pg_tools = PostgreSQLTools()
    
    async def get_tables(self) -> List[Dict[str, str]]:
        """
        获取所有表的信息
        
        Returns:
            表信息列表
        """
        try:
            query = """
            SELECT 
                table_name,
                table_schema,
                CASE 
                    WHEN table_type = 'BASE TABLE' THEN 'table'
                    ELSE lower(table_type)
                END as table_type
            FROM 
                information_schema.tables
            WHERE 
                table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY 
                table_schema, table_name;
            """
            
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.pg_tools.execute_query(query)
            )
            
            return result or []
            
        except Exception as e:
            logger.error(f"Error getting tables: {str(e)}")
            raise
    
    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        执行SQL查询
        
        Args:
            query: SQL查询语句
            
        Returns:
            查询结果行列表
        """
        try:
            if not query.strip():
                return []
                
            # 检查查询是否是只读的
            normalized_query = query.strip().upper()
            if not normalized_query.startswith(('SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN')):
                logger.warning(f"Non-read query attempted: {query}")
                
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.pg_tools.execute_query(query)
            )
            
            return result or []
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    async def import_data(self, file_content: bytes) -> Dict[str, Any]:
        """
        从SQL文件导入数据
        
        Args:
            file_content: SQL文件内容
            
        Returns:
            导入结果
        """
        try:
            # 将字节内容转换为字符串
            sql_content = file_content.decode('utf-8')
            
            # 将SQL文件内容拆分成单独的语句
            sql_statements = self._split_sql_statements(sql_content)
            
            # 执行每个SQL语句
            tables_affected = 0
            loop = asyncio.get_running_loop()
            
            for statement in sql_statements:
                statement = statement.strip()
                if not statement:
                    continue
                
                # 检查是否是创建表或插入语句
                normalized_stmt = statement.upper()
                if 'CREATE TABLE' in normalized_stmt or 'INSERT INTO' in normalized_stmt:
                    tables_affected += 1
                
                await loop.run_in_executor(
                    None,
                    lambda: self.pg_tools.execute_query(statement)
                )
            
            return {
                "success": True,
                "message": f"Successfully imported data. {tables_affected} tables affected.",
                "tables_affected": tables_affected
            }
            
        except Exception as e:
            logger.error(f"Error importing data: {str(e)}")
            raise
    
    async def export_data(self, table_name: str) -> str:
        """
        将表数据导出为CSV格式
        
        Args:
            table_name: 表名
            
        Returns:
            CSV文件路径
        """
        try:
            # 确保表名安全
            if not self._is_valid_table_name(table_name):
                raise ValueError(f"Invalid table name: {table_name}")
            
            # 查询表数据
            query = f"SELECT * FROM {table_name}"
            
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.pg_tools.execute_query(query)
            )
            
            if not result:
                return None
            
            # 创建CSV文件
            output_dir = "output"
            os.makedirs(output_dir, exist_ok=True)
            
            file_path = os.path.join(output_dir, f"{table_name}.csv")
            
            with open(file_path, 'w', newline='') as csvfile:
                if result:
                    # 获取列名
                    fieldnames = result[0].keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    
                    # 写入表头
                    writer.writeheader()
                    
                    # 写入数据行
                    writer.writerows(result)
            
            return file_path
            
        except Exception as e:
            logger.error(f"Error exporting table {table_name}: {str(e)}")
            raise
    
    def _split_sql_statements(self, sql_content: str) -> List[str]:
        """
        将SQL文本拆分为单独的语句
        
        Args:
            sql_content: SQL文本内容
            
        Returns:
            SQL语句列表
        """
        # 简单地按分号拆分
        # 注意：这种方法可能不适用于包含存储过程或复杂语句的SQL
        statements = []
        current_statement = []
        
        for line in sql_content.splitlines():
            line = line.strip()
            
            # 跳过注释行
            if line.startswith('--') or not line:
                continue
                
            current_statement.append(line)
            
            if line.endswith(';'):
                statements.append(' '.join(current_statement))
                current_statement = []
        
        # 添加最后一个语句（如果没有分号结尾）
        if current_statement:
            statements.append(' '.join(current_statement))
        
        return statements
    
    def _is_valid_table_name(self, table_name: str) -> bool:
        """
        检查表名是否有效（防止SQL注入）
        
        Args:
            table_name: 表名
            
        Returns:
            表名是否有效
        """
        # 基本检查：只允许字母、数字、下划线和点（用于schema.table）
        import re
        return bool(re.match(r'^[a-zA-Z0-9_\.]+$', table_name)) 