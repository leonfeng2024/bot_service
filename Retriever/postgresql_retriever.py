from typing import List, Dict, Any
from langchain_community.utilities import SQLDatabase
from Retriever.base_retriever import BaseRetriever
from service.llm_service import LLMService
from langchain.agents import AgentType
from langchain_community.agent_toolkits import SQLDatabaseToolkit, create_sql_agent
from langchain_core.prompts import ChatPromptTemplate, HumanMessagePromptTemplate, SystemMessagePromptTemplate, AIMessagePromptTemplate
import json
import os
from tools.redis_tools import RedisTools
from tools.postgresql_tools import PostgreSQLTools
from tools.token_counter import TokenCounter


class PostgreSQLRetriever(BaseRetriever):
    """PostgreSQL检索器，用于从PostgreSQL数据库中检索数据"""
    
    def __init__(self):
        # 使用PostgreSQLTools代替直接创建连接
        self.pg_tools = PostgreSQLTools()
        
        # 初始化RedisTools
        self.redis_tools = RedisTools()
        
        # 初始化LLM服务
        self.llm_service = LLMService()
        self.llm_service.init_agent_llm("azure-gpt4")
        self.llm = self.llm_service.llm_agent_instance
        
        # 初始化其他变量
        self.toolkit = None
        self.agent = None

    async def retrieve(self, query: str, uuid: str = None) -> List[Dict[str, Any]]:
        """
        从 PostgreSQL 数据库检索相关信息
        
        Args:
            query: 用户查询
            uuid: 会话 ID (可选)
            
        Returns:
            包含检索结果的列表
        """
        try:
            # 记录开始时的token使用情况
            start_usage = self.llm_service.get_token_usage()
            
            # 构建并打印prompt
            prompt = f"PostgreSQL检索查询:\n{query}"
            print(prompt)
            
            # 确保token计数器已初始化
            if not hasattr(self, 'token_counter'):
                self.token_counter = TokenCounter()
            
            # 获取搜索对象（视图和表名列表）
            search_object = self.pg_tools.get_search_objects()
            
            # 创建系统提示
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

                    ユーザーがfieldを提供する場合、以下の手順で検索してください。
                    （1）`field_jpn` または`field` に対して`table_fields`から`physical_name`と`logical_name`を検索してください。　
                    （2）`field_jpn` または`field` に対して`v_view_table_field` から `view_physical_name`と`view_logical_name`を検索してください。　
                    （3）`field_jpn` または`field` に対して`v_dataset_view_table_field` から `ds_physical_name`と`ds_logical_name`を検索してください。　
                    
                    **注意点**：  
                    1. 質問に「影響」「影響範囲」「関係するもの」などの語句が含まれる場合は、すべて（テーブル・ビュー・データセット）を対象にして検索してください。 
                    2. ユーザーが特定の範囲を指定していない場合は、影響範囲を限定せず、すべて（テーブル・ビュー・データセット）を対象にして検索してください。 　
                    
                    ユーザが質問するものだけ回答します。それ以外の情報を回答しないでください。"""

            system_message_prompt = SystemMessagePromptTemplate.from_template(system_template)

            # 创建对话提示
            human_template = """ユーザ問題: {input}"""
            human_message_prompt = HumanMessagePromptTemplate.from_template(human_template)
            ai_template = "考える過程: {agent_scratchpad}"
            ai_message_prompt = AIMessagePromptTemplate.from_template(ai_template)
            chat_prompt = ChatPromptTemplate.from_messages(
                [system_message_prompt, human_message_prompt, ai_message_prompt]
            )

            # 获取数据库对象
            db = self.pg_tools.get_db(include_tables=search_object, view_support=True)
            
            # 确保 self.llm 不为 None
            if self.llm is not None:
                # 创建 SQL 工具包和代理
                self.toolkit = SQLDatabaseToolkit(db=db, llm=self.llm)
                self.agent = create_sql_agent(
                    llm=self.llm,
                    toolkit=self.toolkit,
                    agent_type=AgentType.OPENAI_FUNCTIONS,
                    verbose=False,  # 关闭详细输出以抑制代理输出
                    prompt=chat_prompt,
                    top_k=50000
                )
            else:
                raise ValueError("LLM instance is not initialized properly")

            # 运行代理
            result = await self.agent.ainvoke({"input": query})

            # 提取代理的响应
            postgres_results = [{"content": result['output'], "score": 0.99, "source": "postgresql"}]
            
            # 如果提供了 uuid，将结果存储到 Redis
            if uuid:
                try:
                    # 使用 RedisTools 而不是直接使用 Redis 客户端
                    key = f"{uuid}:postgresql"
                    self.redis_tools.set(key, postgres_results)
                except Exception as redis_error:
                    print(f"Error storing PostgreSQL results in Redis: {str(redis_error)}")
            
            # 记录结束时的token使用情况
            end_usage = self.llm_service.get_token_usage()
            
            # 计算本次调用消耗的token
            input_tokens = end_usage["input_tokens"] - start_usage["input_tokens"]
            output_tokens = end_usage["output_tokens"] - start_usage["output_tokens"]
            
            # 打印token使用情况
            print(f"[PostgreSQL Retriever] Total token usage - Input: {input_tokens} tokens, Output: {output_tokens} tokens")
            
            # 记录token使用情况到计数器
            self.token_counter.total_input_tokens += input_tokens
            self.token_counter.total_output_tokens += output_tokens
            
            # 记录本次调用
            call_record = {
                "source": "postgresql-retriever",
                "model": "azure-gpt4",
                "input_tokens": input_tokens,
                "output_tokens": output_tokens
            }
            self.token_counter.calls_history.append(call_record)
            
            # 添加token使用信息到结果中
            for result in postgres_results:
                result["token_usage"] = {
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens
                }
            
            # 返回结果
            return postgres_results

        except Exception as e:
            import traceback
            print(f"PostgreSQL retriever error: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            
            # 构建错误响应
            error_result = [{"content": f"Error querying PostgreSQL: {str(e)}", "score": 0.0, "source": "postgresql"}]
            
            # 如果提供了 uuid，尝试将错误结果存储到 Redis
            if uuid:
                try:
                    # 使用 RedisTools 而不是直接使用 Redis 客户端
                    key = f"{uuid}:postgresql"
                    self.redis_tools.set(key, [])  # 存储空列表表示没有结果
                except Exception:
                    pass  # 忽略 Redis 存储错误，因为已经出现主要错误
                    
            # 返回错误信息而不是抛出异常
            return error_result

