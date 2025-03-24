import os
from sqlalchemy import create_engine, inspect
from langchain_community.utilities import SQLDatabase
from typing import Dict, Any, List, Optional
import traceback
import logging
from config import (
    POSTGRESQL_DBNAME,
    POSTGRESQL_USER,
    POSTGRESQL_PASSWORD,
    POSTGRESQL_HOST,
    POSTGRESQL_PORT
)

logger = logging.getLogger(__name__)

class PostgreSQLTools:
    def __init__(self):
        """
        初始化PostgreSQL连接工具
        """
        self.pg_host = os.environ.get("POSTGRES_HOST", POSTGRESQL_HOST)
        self.pg_port = int(os.environ.get("POSTGRES_PORT", POSTGRESQL_PORT))
        self.pg_user = os.environ.get("POSTGRES_USER", POSTGRESQL_USER)
        self.pg_password = os.environ.get("POSTGRES_PASSWORD", POSTGRESQL_PASSWORD)
        self.pg_database = os.environ.get("POSTGRES_DB", POSTGRESQL_DBNAME)
        self.db_uri = f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_database}"
        
        self.db_engine = None
        self.db = None
        self.all_views = []
        self.search_object = []
        
        # 测试连接
        self._connect()
        
    def _connect(self) -> None:
        """
        建立到PostgreSQL数据库的连接
        """
        try:
            self.db_engine = create_engine(self.db_uri)
            connection = self.db_engine.connect()
            connection.close()
            logger.info(f"Successfully connected to PostgreSQL database at {self.pg_host}:{self.pg_port}")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            logger.error(traceback.format_exc())
            # 不抛出异常，允许系统继续运行
    
    def get_db_engine(self):
        """
        获取SQLAlchemy引擎
        """
        if self.db_engine is None:
            self._connect()
        return self.db_engine
    
    def get_db(self, include_tables=None, view_support=True):
        """
        获取Langchain SQLDatabase对象
        """
        if self.db is None:
            try:
                self.db = SQLDatabase.from_uri(
                    database_uri=self.db_uri,
                    view_support=view_support,
                    include_tables=include_tables
                )
            except Exception as e:
                logger.error(f"Error initializing SQLDatabase: {str(e)}")
                logger.error(traceback.format_exc())
                raise
        return self.db
    
    def get_search_objects(self):
        """
        获取搜索对象（视图和表）
        """
        try:
            # 确保引擎连接
            engine = self.get_db_engine()
            # 获取所有视图
            self.all_views = inspect(engine).get_view_names()
            # 创建搜索对象列表
            self.search_object = self.all_views + ["table_fields"]
            return self.search_object
        except Exception as e:
            logger.error(f"Error getting search objects: {str(e)}")
            return ["table_fields"]  # 默认返回table_fields表
    
    def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        执行SQL查询
        
        Args:
            query: SQL查询语句
            parameters: 查询参数
            
        Returns:
            查询结果列表
        """
        try:
            engine = self.get_db_engine()
            with engine.connect() as connection:
                if parameters:
                    result = connection.execute(query, parameters)
                else:
                    result = connection.execute(query)
                    
                # 获取列名
                columns = result.keys()
                
                # 转换结果为字典列表
                results = []
                for row in result:
                    results.append(dict(zip(columns, row)))
                    
                return results
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            logger.error(f"Query: {query}")
            logger.error(traceback.format_exc())
            return [] 