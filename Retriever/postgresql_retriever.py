from typing import List, Dict, Any
import traceback
import config
from sqlalchemy import create_engine, inspect
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit, create_sql_agent
from langchain.agents import AgentType
from langchain_core.prompts import ChatPromptTemplate, HumanMessagePromptTemplate, PromptTemplate, SystemMessagePromptTemplate, AIMessagePromptTemplate
from Retriever.base_retriever import BaseRetriever
from service.llm_service import LLMService

class PostgreSQLRetriever(BaseRetriever):
    """PostgreSQL检索器，用于从PostgreSQL数据库检索数据"""
    
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
        self.db_engine = None
        self.toolkit = None
        self.agent = None
        self.all_views = []
        self.search_object = []

    async def retrieve(self, query: str) -> List[Dict[str, Any]]:
        try:
            if self.db_engine is None:
                self.db_engine = create_engine(self.db_uri)

            # Get all views and create search objects
            self.all_views = inspect(self.db_engine).get_view_names()
            self.search_object = self.all_views.append("table_fields")

            system_template = """あなたは SQL データベースの専門家であり、PostgreSQL によるデータ分析を得意としています。
            あなたの任務は、ユーザーのクエリ要求に基づいて、正確かつ効率的な SQL 文を構築することです。
            クエリが正しく、漏れがないことを保証し、以下のルールに従ってください。

            ### **データベースの構造**
            - **`table_fields` テーブル**:  
              - **概要**: 各 `テーブル` の構造情報を格納する。
              - **主なフィールド**:
                - `physical_name` → テーブルの物理名  
                - `logical_name` → テーブルのロジック名（検索には使用しない）  
                - `field` → そのテーブルを構成するカラム名
                - `field_jpn` → そのカラムの日本語名 

            - **`v_view_table_field` ビュー**:  
              - **概要**: 各 `ビュー` の構成情報を格納する。
              - **主なフィールド**:
                - `view_physical_name` → ビューの物理名  
                - `view_logical_name` → ビューのロジック名（検索には使用しない）  
                - `table_physical_name` → そのビューを構成するテーブル名
                - `table_logical_name` → そのテーブルの日本語名 
                - `field` → そのテーブルを構成するカラム名
                - `field_jpn` → そのカラムの日本語名

            - **`v_dataset_view_table_field` ビュー**:
              - **概要**: データセット（`dataset`）の構成情報を格納する。 
              - **主なフィールド**:
                - `ds_physical_name` → データセット名  
                - `ds_logical_name` → データセットのロジック名（検索には使用しない）  
                - `view_name` → そのデータセットを構成するビュー名
                - `table_name` → そのビューを構成するテーブル名
                - `field` → そのテーブルを構成するカラム名
                - `field_jpn` → そのテーブルを構成するカラムの日本語名


            ## **SQL 生成ルール**
            1. どのような質問にも、まず最初に検索用の SQL を作成してください。 
            2. テーブルに関するクエリの場合、`table_fields` のみ使用してください。  
            3. ビューに関するクエリの場合、`v_view_table_field` を使用してください。  
            4. データセットに関するクエリの場合、`v_dataset_view_table_field` を使用してください。  
            5. SQL 文を作成する際は、すべての関連するデータを取得し、必要な情報が漏れないようにしてください。  
            6. ユーザーの質問には必ず日本語で回答してください。  
            7. あなたは私の意見を求める必要はありません。最後まで実行してください。  

            ユーザーが日本語名のfieldを提供する場合、以下の手順で検索してください。
            （1）`field_jpn` に対して`table_fields`から`physical_name`と`logical_name`を検索してください。
            （2）`field_jpn` に対して`v_view_table_field` から `view_physical_name`と`view_logical_name`を検索してください。
            （3）`field_jpn` に対して`v_dataset_view_table_field` から `ds_physical_name`と`ds_logical_name`を検索してください。

            ユーザが質問するものだけ回答します。それ以外の情報を回答しないでください。
            """

            system_message_prompt = SystemMessagePromptTemplate.from_template(system_template)

            # Process the query with the agent
            # Translate the query to make it more SQL-friendly if needed
            # sql_query = f"Based on the database schema, answer this question about the database: {query}"
            human_template = """ユーザ問題: {input}"""
            human_message_prompt = HumanMessagePromptTemplate.from_template(human_template)
            ai_template = "考える過程: {agent_scratchpad}"
            ai_message_prompt = AIMessagePromptTemplate.from_template(ai_template)
            chat_prompt = ChatPromptTemplate.from_messages([system_message_prompt, human_message_prompt, ai_message_prompt])

            # Initialize the database connection if not already done
            if self.db is None:
                self.db = SQLDatabase.from_uri(database_uri=self.db_uri, view_support=True,
                                               include_tables=self.search_object)

                # 确保 self.llm 不为 None
                if self.llm is not None:
                    # Create the SQL toolkit
                    self.toolkit = SQLDatabaseToolkit(db=self.db, llm=self.llm)
                    # Create the SQL agent
                    self.agent = create_sql_agent(
                        llm=self.llm,
                        toolkit=self.toolkit,
                        agent_type=AgentType.OPENAI_FUNCTIONS,
                        verbose=True,
                        prompt=chat_prompt,
                        top_k=50000
                    )
                else:
                    raise ValueError("LLM instance is not initialized properly")

            # Run the agent
            result = await self.agent.ainvoke({"input": query})
            
            # Extract the agent's response
            # agent_response = result.get("output", "No response from SQL agent")
            
            # Return the result in the expected format
            return [{"content": result['output'], "score": 0.99}]
            
        except Exception as e:
            print(f"PostgreSQL retriever error: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            # Return an error message instead of raising an exception
            return [{"content": f"Error querying PostgreSQL: {str(e)}", "score": 0.0}] 