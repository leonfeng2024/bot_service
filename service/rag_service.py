from typing import List, Dict, Any, Optional
from utils.singleton import singleton
from service.llm_service import LLMService
from service.export_excel_service import ExportExcelService
from service.export_ppt_service import ExportPPTService
from tools.redis_tools import RedisTools
from tools.postgresql_tools import PostgreSQLTools
from tools.token_counter import TokenCounter
import json
import sys
import os
import asyncio
import traceback

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Retriever.base_retriever import BaseRetriever
from Retriever.opensearch_retriever import OpenSearchRetriever
from Retriever.postgresql_retriever import PostgreSQLRetriever
from Retriever.neo4j_retriever import Neo4jRetriever
from tools.neo4j_tools import Neo4jTools
from tools.opensearch_tools import OpenSearchTools
from tools.postgresql_tools import PostgreSQLTools

@singleton
class RAGService:
    def __init__(self):
        self.llm_service = LLMService()
        self.llm_service.init_llm("azure-gpt4")
        self.redis_tools = RedisTools()
        self.excel_service = ExportExcelService()
        self.ppt_service = ExportPPTService()
        self.postgresql_tools = PostgreSQLTools()
        # 初始化token计数器
        self.token_counter = TokenCounter()
        pass
    
    async def _multi_source_retrieve(self, query: str, uuid: Optional[str]) -> List[str]:
        retrievers = {
            'opensearch': OpenSearchRetriever(),
            'postgresql': PostgreSQLRetriever(),
            'neo4j': Neo4jRetriever()
        }
        
        json_results = []
        results = []
        doc_counter = 1
        
        for source, retriever in retrievers.items():
            try:
                source_results = await retriever.retrieve(query, uuid)
                
                if source_results and isinstance(source_results, list):
                    for result in source_results:
                        if isinstance(result, dict) and 'content' in result:
                            content = result['content']
                            if not isinstance(content, str):
                                content = str(content)
                            
                            doc_string = f"Doc#{doc_counter}: {content}"
                            results.append(doc_string)
                            
                            result['source'] = source
                            json_results.append(result)
                            
                            doc_counter += 1
                else:
                    error_content = f"从 {source} 检索的结果格式不正确"
                    results.append(f"Doc#{doc_counter}: {error_content}")
                    
                    json_results.append({
                        "content": error_content,
                        "score": 0.5,
                        "source": source
                    })
                    
                    doc_counter += 1
            except Exception as e:
                import traceback
                error_content = f"从 {source} 检索数据时出错: {str(e)}"
                results.append(f"Doc#{doc_counter}: {error_content}")
                
                json_results.append({
                    "content": error_content,
                    "score": 0.4,
                    "source": source
                })
                
                doc_counter += 1
        
        if not results:
            default_content = "未能从任何数据源检索到相关信息"
            results.append(f"Doc#1: {default_content}")
            
            json_results.append({
                "content": default_content,
                "score": 0.3,
                "source": "system"
            })
        
        if uuid:
            await self._store_in_redis(json_results, uuid)

        return results

    async def _rerank(self, results: List[Dict[str, Any]], query: str) -> List[Dict[str, Any]]:
        ranked_results = sorted(results, key=lambda x: x['score'], reverse=True)
        return ranked_results

    async def _process_with_llm(self, docs: List[str], query: str) -> dict:
        try:
            prompt = f"""Answer user questions **strictly** based on the following knowledge base content. Only use provided documents to respond.
 
### Knowledge Base Content  
{chr(10).join([f'- {doc}' for doc in docs])}
 
### User Question  
{query}  
 
### Processing Rules  
1. Return * * in string format only**
2. First, determine whether the table fields included in the user's question match the fields in the knowledge base:
-If the table fields are inconsistent, return 'no' as the category value
-If the table field exists and the table field names are consistent, return 'yes' as the category value
3. * * Prohibited * *:
-Other interpretations beyond String response
-Use external knowledge beyond the provided files 
 
### Response Format  
"no" or "yes"

Example
Knowledge Base:
Doc#1: 表 GetEmployeeDetails 通过字段 employee_id 关联到表 employee_details 的字段 employee_id
Doc#2: 
Procedure 'ABCD_Procedure':
--- ABCD_Procedure
CREATE OR REPLACE FUNCTION ABCD_Procedure(
    id INT,
    ok_key DECIMAL(10, 2),
    changed_by INT 
) RETURNS VOID AS $$
DECLARE 
    old_salary DECIMAL(10, 2);
BEGIN 
    SELECT ok_key INTO old_salary FROM ABCD_no WHERE oder_key = id;
    UPDATE ABCD_no SET ok_key = ok_key WHERE oder_key = id;
    INSERT INTO ABCD_noi (
        ok_id,
        key_id,
        ok_key,
        oder_key 
    ) VALUES (
        ok_id,
        key_id,
        ok_key,
        oder_key 
    );
END;
$$ LANGUAGE plpgsql;

User Question:
"I want to change column [avg_file_size] of table documents_info "

Response:
"no"

Reason 1: the columns in knowledge base are not as same as [avg_file_size].
Reason 2: column [avg_file_size] is bot exist in knowledge base.
"""
            
            llm = self.llm_service.get_llm()
            response = await llm.generate(prompt)
            
            cleaned_response = response.strip().lower()
            cleaned_response = cleaned_response.replace('"', '').replace("'", '')
            cleaned_response = cleaned_response.split('\n')[0]
            
            if "yes" in cleaned_response:
                final_check = "yes"
            elif "no" in cleaned_response:
                final_check = "no"
            else:
                final_check = "unknown"
            
            return {
                "final_check": final_check,
                "answer": response
            }
            
        except Exception as e:
            pass
            return {
                "final_check": "unknown",
                "answer": f"Error processing your query: {str(e)}"
            }

    async def _generate_excel_and_ppt(self, query: str, uuid: str) -> str:
        try:
            os.makedirs(self.ppt_service.output_dir, exist_ok=True)
            def process_redis_data(data: Any) -> List[Dict[str, Any]]:
                if not data:
                    return []
                if isinstance(data, str):
                    try:
                        import json
                        data = json.loads(data)
                    except Exception as e:
                        return []
                if isinstance(data, dict):
                    data = [data]
                if not isinstance(data, list):
                    return []
                return [item for item in data if isinstance(item, dict)]
            
            neo4j_data = process_redis_data(self.redis_tools.get(f"{uuid}:neo4j"))
            data_to_use: List[Dict[str, Any]] = []
            data_source = None
            
            if neo4j_data:
                data_to_use = neo4j_data
                data_source = "neo4j"
            else:
                opensearch_data = process_redis_data(self.redis_tools.get(f"{uuid}:opensearch"))
                if opensearch_data:
                    data_to_use = opensearch_data
                    data_source = "opensearch"
                else:
                    postgresql_data = process_redis_data(self.redis_tools.get(f"{uuid}:postgresql"))
                    if postgresql_data:
                        data_to_use = postgresql_data
                        data_source = "postgresql"
            
            if data_to_use:
                if len(data_to_use) == 0 or not isinstance(data_to_use[0], dict):
                    data_to_use = [
                        {"content": "Generated dummy data 1", "score": 0.95, "source": data_source},
                        {"content": "Generated dummy data 2", "score": 0.90, "source": data_source}
                    ]
                
                filename_prefix = f"{uuid}_{data_source}"
                excel_path = os.path.join(self.ppt_service.output_dir, f"{filename_prefix}.xlsx")
                
                excel_file = await self.excel_service.export_to_excel(data_to_use, filename_prefix)
                
                if not os.path.exists(excel_file):
                    import pandas as pd
                    df = pd.DataFrame(data_to_use)
                    excel_path = os.path.join(self.ppt_service.output_dir, f"{filename_prefix}.xlsx")
                    df.to_excel(excel_path, index=False)
                    excel_file = excel_path
                
                ppt_file = await self.ppt_service.export_to_ppt(excel_file, filename_prefix)
                
                if not os.path.exists(ppt_file):
                    return "Error: PPT file was not created"
                
                if data_source != "opensearch":
                    opensearch_data = self.redis_tools.get(f"{uuid}:opensearch")
                    if opensearch_data and isinstance(opensearch_data, list) and len(opensearch_data) > 0:
                        try:
                            ppt_file = await self.ppt_service.append_to_ppt(opensearch_data, ppt_file)
                        except Exception:
                            pass
                
                full_path = os.path.abspath(ppt_file)
                
                return os.path.basename(ppt_file)
            else:
                return "No data available to generate documents"
                
        except Exception as e:
            import traceback
            pass
            return f"Error generating document: {str(e)}"

    async def _store_in_redis(self, results: List[Dict[str, Any]], uuid: str):
        try:
            grouped_results = {}
            for result in results:
                source = result.get('source', 'unknown')
                if source not in grouped_results:
                    grouped_results[source] = []
                grouped_results[source].append(result)
                
            for source, source_results in grouped_results.items():
                try:
                    key = f"{uuid}:{source}"
                    self.redis_tools.set(key, source_results)
                    print(f"Successfully stored {source} results in Redis with key: {key}")
                except Exception as source_error:
                    print(f"Error storing {source} results in Redis: {str(source_error)}")
        except Exception as e:
            import traceback
            print(f"Error in _store_in_redis: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")

    async def retrieve(self, query: str, uuid: Optional[str] = None) -> Dict[str, Any]:
        """检索相关文档并生成回答"""
        try:
            print(f"Starting RAG process for query: {query}, uuid: {uuid}")
            
            # 获取文档
            docs = await self._multi_source_retrieve(query, uuid)
            print("Retrieved documents:")
            for doc in docs[:3]:
                print(f"  {doc[:100]}..." if len(doc) > 100 else f"  {doc}")
            print(f"  ... (total {len(docs)} documents)")
            
            # 使用LLM处理文档
            llm_result = await self._process_with_llm(docs, query)
            
            # 获取token使用情况
            token_usage = self.llm_service.get_formatted_token_usage()
            
            # 构建响应
            response = {
                "status": "success",
                "docs": docs,
                "answer": llm_result.get("answer", ""),
                "final_check": llm_result.get("final_check", "unknown"),
                "token_usage": token_usage
            }
            
            return response
        except Exception as e:
            import traceback
            error_msg = f"Error in RAG service: {str(e)}\n{traceback.format_exc()}"
            print(error_msg)
            return {
                "status": "error",
                "error": error_msg,
                "token_usage": self.llm_service.get_formatted_token_usage()
            }
        
        if not results:
            return {"status": "error", "message": "No results found"}
        
        llm_response = await self._process_with_llm(results, query)
        doc_path = None
        if llm_response['final_check'] == "yes":
            print(f"final_check is 'yes', generating Excel and PPT files for uuid: {uuid}")
            doc_path = await self._generate_excel_and_ppt(query, uuid)
            print(f"Document generation result: {doc_path}")

        response = {
            "status": "success",
            "message": llm_response
        }

        if doc_path and doc_path not in ["No data available to generate documents", "Error: PPT file was not created"] and not doc_path.startswith("Error"):
            print(f"Adding document path to response: {doc_path}")
            output_dir = os.path.abspath(self.ppt_service.output_dir)
            full_file_path = os.path.join(output_dir, doc_path)
            
            if os.path.exists(full_file_path):
                response["document"] = {
                    "file_name": doc_path,
                    "file_path": full_file_path
                }
                print(f"Document added to response: {response['document']}")
            else:
                print(f"Warning: Generated file does not exist at path: {full_file_path}")
                response["error"] = f"Failed to generate document: file not found at {full_file_path}"
        elif doc_path:
            response["error"] = doc_path
            print(f"Error in document generation: {doc_path}")
            
        return response

    async def _save_chat_history(self, query: str, uuid: str, user_id: str):
        print(f"_save_chat_history : {uuid}, {user_id}, {query}")
        try:
            user_query_sql = """
            INSERT INTO user_chat_history 
            (user_id, uuid, user_query, response, sender_role) 
            VALUES (%(user_id)s, %(uuid)s, %(user_query)s, %(response)s, 'user')
            """
            self.postgresql_tools.execute_query(
                user_query_sql,
                parameters={"user_id": user_id, "uuid": uuid, "user_query": query, "response": ""}
            )
            
            data_sources = ['neo4j', 'opensearch', 'postgresql']
            for source in data_sources:
                source_data = self.redis_tools.get(f"{uuid}:{source}")
                if source_data:
                    response_content = json.dumps(source_data) if isinstance(source_data, (list, dict)) else str(source_data)
                    assistant_response_sql = """
                    INSERT INTO user_chat_history 
                    (user_id, uuid, user_query, response, sender_role) 
                    VALUES (%(user_id)s, %(uuid)s, %(user_query)s, %(response)s, 'assistant')
                    """
                    self.postgresql_tools.execute_query(
                        assistant_response_sql,
                        parameters={"user_id": user_id, "uuid": uuid, "user_query": query, "response": response_content}
                    )
        except Exception as e:
            print(f"Error saving chat history: {str(e)}")
            print(traceback.format_exc())