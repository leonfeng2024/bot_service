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
        Get information about all tables
        
        Returns:
            List of table information
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
    
    async def execute_query(self, query: str, parameters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Execute SQL query
        
        Args:
            query: SQL query statement
            parameters: Optional query parameters
            
        Returns:
            List of query result rows
        """
        try:
            if not query.strip():
                return []
                
            # Check if query is read-only
            normalized_query = query.strip().upper()
            is_read_only = normalized_query.startswith(('SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN'))
            
            # Only check for read-only if no parameters are provided (DML with parameters is allowed)
            if not is_read_only and not parameters and not "RETURNING" in normalized_query:
                logger.warning(f"Non-read query attempted: {query}")
                
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.pg_tools.execute_query(query, parameters)
            )
            
            return result or []
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    async def import_data(self, file_content: bytes) -> Dict[str, Any]:
        """
        Import data from SQL file
        
        Args:
            file_content: SQL file content
            
        Returns:
            Import result
        """
        try:
            # Convert bytes content to string
            sql_content = file_content.decode('utf-8')
            
            # Split SQL file content into individual statements
            sql_statements = self._split_sql_statements(sql_content)
            
            # Execute each SQL statement
            tables_affected = 0
            loop = asyncio.get_running_loop()
            
            for statement in sql_statements:
                statement = statement.strip()
                if not statement:
                    continue
                
                # Check if it's a CREATE TABLE or INSERT statement
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
        Export table data to CSV format
        
        Args:
            table_name: Table name
            
        Returns:
            CSV file path
        """
        try:
            # Ensure table name is safe
            if not self._is_valid_table_name(table_name):
                raise ValueError(f"Invalid table name: {table_name}")
            
            # Query table data
            query = f"SELECT * FROM {table_name}"
            
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.pg_tools.execute_query(query)
            )
            
            if not result:
                return None
            
            # Create CSV file
            output_dir = "output"
            os.makedirs(output_dir, exist_ok=True)
            
            file_path = os.path.join(output_dir, f"{table_name}.csv")
            
            with open(file_path, 'w', newline='') as csvfile:
                if result:
                    # Get column names
                    fieldnames = result[0].keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    
                    # Write header
                    writer.writeheader()
                    
                    # Write data rows
                    writer.writerows(result)
            
            return file_path
            
        except Exception as e:
            logger.error(f"Error exporting table {table_name}: {str(e)}")
            raise
    
    def _split_sql_statements(self, sql_content: str) -> List[str]:
        """
        Split SQL text into individual statements
        
        Args:
            sql_content: SQL text content
            
        Returns:
            List of SQL statements
        """
        # Simply split by semicolon
        # Note: This method may not be suitable for SQL containing stored procedures or complex statements
        statements = []
        current_statement = []
        
        for line in sql_content.splitlines():
            line = line.strip()
            
            # Skip comment lines
            if line.startswith('--') or not line:
                continue
                
            current_statement.append(line)
            
            if line.endswith(';'):
                statements.append(' '.join(current_statement))
                current_statement = []
        
        # Add the last statement (if it doesn't end with semicolon)
        if current_statement:
            statements.append(' '.join(current_statement))
        
        return statements
    
    def _is_valid_table_name(self, table_name: str) -> bool:
        """
        Check if table name is valid (prevent SQL injection)
        
        Args:
            table_name: Table name
            
        Returns:
            Whether table name is valid
        """
        # Basic check: only allow letters, numbers, underscores and dots (for schema.table)
        import re
        return bool(re.match(r'^[a-zA-Z0-9_\.]+$', table_name)) 