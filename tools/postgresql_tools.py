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
        Initialize PostgreSQL connection tool
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
        
        # No tables excluded by default, can be configured as needed
        self.include_tables = None
        self.exclude_tables = None
        
        # Test connection
        self._connect()
        
    def _connect(self) -> None:
        """
        Establish connection to PostgreSQL database using connection pool
        """
        try:
            if not self.db_engine:
                # Create engine with connection pool
                self.db_engine = create_engine(
                    self.db_uri,
                    poolclass=QueuePool,
                    pool_size=5,  # Connection pool size
                    max_overflow=10,  # Maximum overflow connections
                    pool_timeout=30,  # Connection timeout (seconds)
                    pool_recycle=1800  # Connection recycle time (seconds)
                )
            
            # Use context manager pattern to test connection
            if self.db_engine is not None:
                # Use with statement to ensure proper connection closure
                with self.db_engine.connect() as connection:
                    # Connection successfully established
                    logger.info(f"Successfully connected to PostgreSQL database at {self.pg_host}:{self.pg_port} with connection pool")
            else:
                logger.warning("Database engine is None")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            logger.error(traceback.format_exc())
            # Don't raise exception, allow system to continue running
    
    def get_sqlalchemy_uri(self) -> str:
        """
        Return database URI string for SQLAlchemy connection
        
        Returns:
            str: SQLAlchemy connection URI string
        """
        return self.db_uri
    
    def get_db_engine(self):
        """
        Get SQLAlchemy engine
        """
        if self.db_engine is None:
            self._connect()
        return self.db_engine
    
    def get_db(self, include_tables=None, view_support=True):
        """
        Get Langchain SQLDatabase object
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
        Get search objects (views and tables)
        
        Returns:
            List[str]: List containing all view names and table_fields
        """
        try:
            # Ensure engine connection
            engine = self.get_db_engine()
            if engine is None:
                logger.warning("Database engine is None, returning default list")
                return ["table_fields"]
            
            # Use context manager pattern for connection
            with engine.connect() as connection:
                inspector = inspect(engine)
                if inspector is None:
                    logger.warning("Inspector is None, returning default list")
                    return ["table_fields"]
                
                # Get all views using correct method call
                try:
                    # Use schema=None as default parameter for get_view_names
                    views = inspector.get_view_names(schema=None)
                    self.all_views = [] if views is None else list(views)
                except Exception as view_error:
                    logger.warning(f"Error getting view names: {str(view_error)}")
                    self.all_views = []
                
                # Create search objects list
                self.search_object = self.all_views + ["table_fields"]
                return self.search_object
        except Exception as e:
            logger.error(f"Error getting search objects: {str(e)}")
            logger.error(traceback.format_exc())
            return ["table_fields"]  # Return table_fields table by default
    
    def execute_auth_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        logger.debug(f"Executing authentication query: {query}")
        logger.debug(f"Query parameters: {parameters}")
        """
        Dedicated SQL query execution function for user authentication
        
        Args:
            query: SQL query statement
            parameters: Query parameters
            
        Returns:
            First row of query results, or None if no results or error occurs
        """
        try:
            from sqlalchemy import text
            # Ensure engine connection
            if self.db_engine is None:
                self._connect()
                
            if not self.db_engine:
                logger.error("Database engine not available for authentication query")
                return None
            
            # Use context manager pattern for connection
            with self.db_engine.connect() as connection:
                # Convert query to SQLAlchemy text object
                sql = text(query)
                # Execute query
                result = connection.execute(sql, parameters if parameters else {})
                # Get first row of results
                return result.fetchone()
                
        except Exception as e:
            logger.error(f"Error executing authentication query: {str(e)}")
            logger.error(f"Query: {query}")
            logger.error(traceback.format_exc())
            return None

    def validate_user_credentials(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        try:
            # Build query
            query = """
                SELECT user_id, role, isactive AS is_active 
                FROM user_info 
                WHERE username = :username 
                AND password = :password
            """
            
            # Execute using dedicated authentication query function
            user_data = self.execute_auth_query(query, {"username": username, "password": password})
            
            # Validate results
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
            
            # Use context manager pattern for connection and transaction
            with engine.connect() as connection:
                # Create transaction
                with connection.begin() as transaction:
                    try:
                        # Check if it's a SELECT statement
                        is_select = query.strip().upper().startswith("SELECT")
                        
                        # Convert %(name)s format parameters to :name format
                        if parameters:
                            # Replace parameter format
                            modified_query = query
                            for key in parameters.keys():
                                modified_query = modified_query.replace(f'%({key})s', f':{key}')
                            sql = text(modified_query)
                            result = connection.execute(sql, parameters)
                        else:
                            # Convert query to SQLAlchemy text object
                            sql = text(query)
                            result = connection.execute(sql)
                        
                        if is_select:
                            # Get column names
                            columns = result.keys()
                            
                            # Convert results to list of dictionaries
                            results = []
                            for row in result:
                                results.append(dict(zip(columns, row)))
                            
                            # Transaction will be automatically committed at the end of with block
                            return results
                        else:
                            # For non-SELECT statements (INSERT/UPDATE/DELETE), ensure transaction is committed
                            # Transaction will be automatically committed at the end of with block
                            return []
                    except Exception as inner_e:
                        # If error occurs during execution, log and re-raise
                        logger.error(f"Error during query execution: {str(inner_e)}")
                        logger.error(f"Query: {query}")
                        logger.error(traceback.format_exc())
                        # Transaction will be automatically rolled back on exception
                        raise
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            logger.error(f"Query: {query}")
            logger.error(traceback.format_exc())
            return []
