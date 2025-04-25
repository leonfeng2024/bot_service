import os
from sqlalchemy import create_engine, inspect
from sqlalchemy.pool import QueuePool
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
        
        # 默认不排除任何表，可以根据需要设置
        self.include_tables = None
        self.exclude_tables = None
        
        # 测试连接
        self._connect()
        
    def _connect(self) -> None:
        """
        建立到PostgreSQL数据库的连接，使用连接池
        """
        try:
            if not self.db_engine:
                # 创建带有连接池的引擎
                self.db_engine = create_engine(
                    self.db_uri,
                    poolclass=QueuePool,
                    pool_size=5,  # 连接池大小
                    max_overflow=10,  # 最大溢出连接数
                    pool_timeout=30,  # 连接超时时间（秒）
                    pool_recycle=1800  # 连接回收时间（秒）
                )
            
            # 使用上下文管理器模式测试连接
            if self.db_engine is not None:
                # 使用with语句确保连接正确关闭
                with self.db_engine.connect() as connection:
                    # 连接成功建立
                    logger.info(f"Successfully connected to PostgreSQL database at {self.pg_host}:{self.pg_port} with connection pool")
            else:
                logger.warning("Database engine is None")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            logger.error(traceback.format_exc())
            # 不抛出异常，允许系统继续运行
    
    def get_sqlalchemy_uri(self) -> str:
        """
        返回用于SQLAlchemy连接的数据库URI字符串
        
        Returns:
            str: SQLAlchemy连接URI字符串
        """
        return self.db_uri
    
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
    
    def get_search_objects(self) -> List[str]:
        """
        获取搜索对象（视图和表）
        
        Returns:
            List[str]: 包含所有视图名称和table_fields的列表
        """
        try:
            # 确保引擎连接
            engine = self.get_db_engine()
            if engine is None:
                logger.warning("Database engine is None, returning default list")
                return ["table_fields"]
            
            # 使用上下文管理器模式处理连接
            with engine.connect() as connection:
                inspector = inspect(engine)
                if inspector is None:
                    logger.warning("Inspector is None, returning default list")
                    return ["table_fields"]
                
                # 获取所有视图，使用正确的方法调用
                try:
                    # 使用schema=None作为默认参数调用get_view_names
                    views = inspector.get_view_names(schema=None)
                    self.all_views = [] if views is None else list(views)
                except Exception as view_error:
                    logger.warning(f"Error getting view names: {str(view_error)}")
                    self.all_views = []
                
                # 创建搜索对象列表
                self.search_object = self.all_views + ["table_fields"]
                return self.search_object
        except Exception as e:
            logger.error(f"Error getting search objects: {str(e)}")
            logger.error(traceback.format_exc())
            return ["table_fields"]  # 默认返回table_fields表
    
    def execute_auth_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        logger.debug(f"Executing authentication query: {query}")
        logger.debug(f"Query parameters: {parameters}")
        """
        专门用于用户认证的SQL查询执行函数
        
        Args:
            query: SQL查询语句
            parameters: 查询参数
            
        Returns:
            查询结果的第一行，如果没有结果或发生错误则返回None
        """
        try:
            from sqlalchemy import text
            # 确保引擎连接
            if self.db_engine is None:
                self._connect()
                
            if not self.db_engine:
                logger.error("Database engine not available for authentication query")
                return None
            
            # 使用上下文管理器模式处理连接
            with self.db_engine.connect() as connection:
                # 将查询转换为SQLAlchemy text对象
                sql = text(query)
                # 执行查询
                result = connection.execute(sql, parameters if parameters else {})
                # 获取第一行结果
                return result.fetchone()
                
        except Exception as e:
            logger.error(f"Error executing authentication query: {str(e)}")
            logger.error(f"Query: {query}")
            logger.error(traceback.format_exc())
            return None
    def validate_user_credentials(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        try:
            # 构建查询
            query = """
                SELECT user_id, role, isactive AS is_active 
                FROM user_info 
                WHERE username = :username 
                AND password = :password
            """
            
            # 使用专用的认证查询函数执行
            user_data = self.execute_auth_query(query, {"username": username, "password": password})
            
            # 验证结果
            if user_data and user_data.is_active:
                return {"user_id": user_data.user_id, "role": user_data.role}
            return None

        except Exception as e:
            logger.error(f"Authentication error for {username}: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        try:
            from sqlalchemy import text
            engine = self.get_db_engine()
            if not engine:
                raise RuntimeError("Database engine is not available")
            
            # 使用上下文管理器模式处理连接和事务
            with engine.connect() as connection:
                # 创建事务
                with connection.begin() as transaction:
                    try:
                        # 检查是否是SELECT语句
                        is_select = query.strip().upper().startswith("SELECT")
                        
                        # 将%(name)s格式的参数转换为:name格式
                        if parameters:
                            # 替换参数格式
                            modified_query = query
                            for key in parameters.keys():
                                modified_query = modified_query.replace(f'%({key})s', f':{key}')
                            sql = text(modified_query)
                            result = connection.execute(sql, parameters)
                        else:
                            # 将查询转换为SQLAlchemy text对象
                            sql = text(query)
                            result = connection.execute(sql)
                        
                        if is_select:
                            # 获取列名
                            columns = result.keys()
                            
                            # 转换结果为字典列表
                            results = []
                            for row in result:
                                results.append(dict(zip(columns, row)))
                            
                            # 事务会在with块结束时自动提交
                            return results
                        else:
                            # 对于非SELECT语句（如INSERT/UPDATE/DELETE），确保事务提交
                            # 事务会在with块结束时自动提交
                            return []
                    except Exception as inner_e:
                        # 如果执行过程中出现错误，记录并重新抛出
                        logger.error(f"Error during query execution: {str(inner_e)}")
                        logger.error(f"Query: {query}")
                        logger.error(traceback.format_exc())
                        # 事务会在异常时自动回滚
                        raise
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            logger.error(f"Query: {query}")
            logger.error(traceback.format_exc())
            return []
