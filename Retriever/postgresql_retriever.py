from typing import List, Dict, Any, Optional
from langchain_community.utilities import SQLDatabase
from Retriever.base_retriever import BaseRetriever
from service.llm_service import LLMService
from langchain.agents import AgentType
from langchain_community.agent_toolkits import SQLDatabaseToolkit, create_sql_agent
from langchain_core.prompts import ChatPromptTemplate, HumanMessagePromptTemplate, SystemMessagePromptTemplate, AIMessagePromptTemplate
import json
import os
import time
from tools.redis_tools import RedisTools
from tools.postgresql_tools import PostgreSQLTools
from tools.token_counter import TokenCounter


class PostgreSQLRetriever(BaseRetriever):
    
    def __init__(self):
        self.pg_tools = PostgreSQLTools()
        self.redis_tools = RedisTools()
        self.llm_service = LLMService()
        self.llm_service.init_agent_llm("openai-gpt-4.1")
        self.llm = self.llm_service.llm_agent_instance
        self.request_timeout = int(os.environ.get("PG_REQUEST_TIMEOUT", 300))
        self.max_retries = int(os.environ.get("PG_MAX_RETRIES", 1)) 
        self.retry_delay = int(os.environ.get("PG_RETRY_DELAY", 2))
        self.cache_ttl = int(os.environ.get("PG_CACHE_TTL", 3600))
        self.use_cache = os.environ.get("PG_USE_CACHE", "True").lower() == "true"
        self.token_counter = TokenCounter()
        self.toolkit = None
        self.agent = None
        
    def _save_to_cache(self, query: str, uuid: str, results: List[Dict[str, Any]]) -> None:

        if not self.use_cache or not uuid:
            return
            
        cache_key = f"pg_cache:{uuid}:{hash(query)}"
        self.redis_tools.set(cache_key, results, expire=self.cache_ttl)
        print(f"[PostgreSQL Retriever] Saved to cache: {query[:50]}...")

    async def retrieve(self, query: str, uuid: str = None) -> List[Dict[str, Any]]:
        try:
            start_usage = self.llm_service.get_token_usage()
            start_time = time.time()
            
            prompt = f"PostgreSQL Retrieval Query:\n{query}"
            print(prompt)
            
            search_object = self.pg_tools.get_search_objects()
            print("search_object:")
            print(search_object)
            system_template = """
            You are an SQL database expert specializing in data analysis with PostgreSQL. 
            Your mission is to construct accurate and efficient SQL statements based on user query requirements. 
            Ensure the queries are correct and complete, following these rules: 

                Important rules:  
                - you should judge the user's question is about table name or field name.
                - if the user's question is about table name, you should use `physical_name`,'view_physical_name','ds_physical_name' or `logical_name`,'view_logical_name','ds_logical_name' as where clause to generate the SQL statement.
                - if the user's question is about field,column name, you should use `field` or `field_jpn` as where clause to generate the SQL statement.

                ### **Database Structure**
                - **`table_fields` table**:  
                  - **Overview**: Stores structural information of each table field`.
                  - **Key fields**:
                    - `physical_name` → Physical table name
                    - `logical_name` → Logical table name 
                    - `field` → Column name in the table 
                    - `field_jpn` → Japanese name of the column 

                - **`v_view_table_field` view**:  
                  - **Overview**: Stores composition information of each `view table field`.
                  - **Key fields**:
                    - `view_physical_name` → Physical view name 
                    - `view_logical_name` → Logical view name 
                    - `table_physical_name` → Table name composing the view 
                    - `table_logical_name` → Japanese name of the table 
                    - `field` → Column name in the table 
                    - `field_jpn` → Japanese name of the column 

                - **`v_dataset_view_table_field` view**:
                  - **Overview**: Stores composition information of datasets (`dataset`). 
                  - **Key fields**:
                    - `ds_physical_name` → Dataset name
                    - `ds_logical_name` → Logical dataset name 
                    - `view_name` → View name composing the dataset 
                    - `table_name` → Table name composing the view 
                    - `field` → Column name in the table 
                    - `field_jpn` → Japanese name of the column 


                ## **SQL Generation Rules**
                1. For any question, first create a search SQL statement. 
                2. For table field-related queries, use only `table_fields`.  
                3. For view table field-related queries, use `v_view_table_field`.  
                4. For dataset view table field-related queries, use `v_dataset_view_table_field`.  
                5. When creating SQL statements, retrieve all relevant data to ensure no necessary information is missing.  
                6. Always respond to user questions in Japanese.  
                7. You don't need to consult my opinion. Execute through to the end.  
                
                **Important Notes**:  
                1. Determine the intent of the user's input. If it contains terms like "impact", "scope of impact", or "related items" etc., you should recognize that the user intends to obtain impact-related content. Search all objects (tables/views/datasets).
                2. When users don't specify a scope, search all objects (tables/views/datasets) without limiting the impact range. 
                
                Respond only to what the user asks. Do not provide any additional information.The final returned results, not the SQL statements you generated. Please execute the generated SQL statements, organize the returned content, and provide it in string list format.
                """

            system_message_prompt = SystemMessagePromptTemplate.from_template(system_template)

            human_template = """User question: {input}"""
            human_message_prompt = HumanMessagePromptTemplate.from_template(human_template)
            ai_template = "Thinking process: {agent_scratchpad}"
            ai_message_prompt = AIMessagePromptTemplate.from_template(ai_template)
            chat_prompt = ChatPromptTemplate.from_messages(
                [system_message_prompt, human_message_prompt, ai_message_prompt]
            )

            db = self.pg_tools.get_db(include_tables=search_object, view_support=True)
            
            self.toolkit  = SQLDatabaseToolkit(db=db, llm=self.llm) 
            self.agent  = create_sql_agent(
                llm=self.llm, 
                toolkit=self.toolkit, 
                agent_type=AgentType.OPENAI_FUNCTIONS,
                verbose=True,
                max_iterations=15, 
                max_execution_time=60, 
                top_k=2000,
                prompt=chat_prompt,
                handle_parsing_errors=True,
                stop_sequence=["NO_MORE_QUERIES_NEEDED"]
            )

            retries = 0
            result = None
            last_error = None
            
            while retries < self.max_retries:
                try:
                    result = await self.agent.ainvoke(
                        {"input": query}
                    )
                    break
                except Exception as e:
                    last_error = e
                    retries += 1
                    if retries < self.max_retries:
                        print(f"[PostgreSQL Retriever] Attempt {retries} failed: {str(e)}. Retrying in {self.retry_delay}s...")
                        time.sleep(self.retry_delay)
                    else:
                        print(f"[PostgreSQL Retriever] All {self.max_retries} attempts failed.")
            
            if result is None:
                raise last_error or Exception("Failed to execute agent after multiple retries")
            else:
                print(f"[PostgreSQL Retriever] Agent executed successfully.")
                print(result['output'])
            postgres_results = [{"content": result['output'], "score": 0.99, "source": "postgresql"}]
            
            if uuid:
                try:
                    key = f"{uuid}:postgresql"
                    self.redis_tools.set(key, postgres_results)
                except Exception as redis_error:
                    print(f"Error storing PostgreSQL results in Redis: {str(redis_error)}")
            
            end_usage = self.llm_service.get_token_usage()
            end_time = time.time()
            
            input_tokens = end_usage["input_tokens"] - start_usage["input_tokens"]
            output_tokens = end_usage["output_tokens"] - start_usage["output_tokens"]
            execution_time = end_time - start_time
            
            print(f"[PostgreSQL Retriever] Total token usage - Input: {input_tokens} tokens, Output: {output_tokens} tokens")
            print(f"[PostgreSQL Retriever] Execution time: {execution_time:.2f} seconds")
            
            self.token_counter.total_input_tokens += input_tokens
            self.token_counter.total_output_tokens += output_tokens
            
            call_record = {
                "source": "postgresql-retriever",
                "model": "azure-gpt4",
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "execution_time": execution_time
            }
            self.token_counter.calls_history.append(call_record)
            
            for result in postgres_results:
                result["token_usage"] = {
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "execution_time": execution_time
                }
            
            self._save_to_cache(query, uuid, postgres_results)
            
            return postgres_results

        except Exception as e:
            import traceback
            print(f"PostgreSQL retriever error: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            
            error_result = [{"content": f"Error querying PostgreSQL: {str(e)}", "score": 0.0, "source": "postgresql"}]
            
            if uuid:
                try:
                    key = f"{uuid}:postgresql"
                    self.redis_tools.set(key, [])
                except Exception:
                    pass
                    
            return error_result

