from typing import List, Dict, Any, Optional, AsyncGenerator
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
        # Don't specify LLM type, let system use global configuration
        self.llm_service.init_llm()
        self.redis_tools = RedisTools()
        self.excel_service = ExportExcelService()
        self.ppt_service = ExportPPTService()
        self.postgresql_tools = PostgreSQLTools()
        # Initialize token counter
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
                    error_content = f"Invalid format of results retrieved from {source}"
                    results.append(f"Doc#{doc_counter}: {error_content}")
                    
                    json_results.append({
                        "content": error_content,
                        "score": 0.5,
                        "source": source
                    })
                    
                    doc_counter += 1
                
                # Return completion status for each retriever
                yield {"step": f"{source}_retriever", "message": f"{source.capitalize()} database query completed"}
                
            except Exception as e:
                import traceback
                error_content = f"Error retrieving data from {source}: {str(e)}"
                results.append(f"Doc#{doc_counter}: {error_content}")
                
                json_results.append({
                    "content": error_content,
                    "score": 0.4,
                    "source": source
                })
                
                doc_counter += 1
        
        if not results:
            default_content = "No relevant information retrieved from any data source"
            results.append(f"Doc#1: {default_content}")
            
            json_results.append({
                "content": default_content,
                "score": 0.3,
                "source": "system"
            })
        
        if uuid:
            await self._store_in_redis(json_results, uuid)

        yield results

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
Doc#1: Table GetEmployeeDetails is linked to table employee_details through field employee_id
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
            
            # Return LLM processing result
            yield {"step": "_process_with_llm", "message": response}
            
            yield {
                "final_check": final_check,
                "answer": response
            }
            
        except Exception as e:
            pass
            yield {
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

    async def retrieve(self, query: str, uuid: Optional[str] = None) -> AsyncGenerator[Dict[str, Any], None]:
        """Retrieve relevant documents and generate answer"""
        try:
            print(f"Starting RAG process for query: {query}, uuid: {uuid}")
            
            # Return process start message
            yield {"step": "process_start", "message": "Starting to process query"}
            
            # Get documents
            docs = []
            async for doc_batch in self._multi_source_retrieve(query, uuid):
                # Check if this is a status update
                if isinstance(doc_batch, dict) and "step" in doc_batch:
                    yield doc_batch
                    continue
                docs = doc_batch
            
            # Return document retrieval completion status
            yield {"step": "docs_retrieved", "message": f"Retrieved {len(docs)} documents"}
            
            print("Retrieved documents:")
            for doc in docs[:3]:
                print(f"  {doc[:100]}..." if len(doc) > 100 else f"  {doc}")
            print(f"  ... (total {len(docs)} documents)")
            
            # Process documents with LLM
            llm_result = None
            async for result in self._process_with_llm(docs, query):
                if isinstance(result, dict):
                    if "step" in result:
                        yield result
                    else:
                        llm_result = result
            
            if not llm_result:
                llm_result = {"final_check": "unknown", "answer": "Failed to process query"}
            
            # Return LLM processing completion status
            yield {"step": "llm_process_complete", "message": "LLM processing completed"}
            
            # Get token usage
            token_usage = self.llm_service.get_formatted_token_usage()
            
            # If final_check is yes, generate Excel and PPT documents
            if llm_result.get("final_check") == "yes" and uuid:
                # Return document generation start status
                yield {"step": "generating_document", "message": "Generating documents..."}
                
                print(f"final_check is 'yes', generating Excel and PPT files for uuid: {uuid}")
                doc_path = None
                async for path in self._generate_document(uuid):
                    doc_path = path
                print(f"Document generation result: {doc_path}")
                
                # If document was generated, add document info to response
                if doc_path and doc_path not in ["No data available to generate documents", "Error: PPT file was not created"] and not doc_path.startswith("Error"):
                    print(f"Adding document path to response: {doc_path}")
                    output_dir = os.path.abspath(self.ppt_service.output_dir)
                    full_file_path = os.path.join(output_dir, doc_path)
                    
                    if os.path.exists(full_file_path):
                        # Generate markdown format link
                        file_name = os.path.basename(full_file_path)
                        file_url = f"http://localhost:8088/output/{file_name}"
                        markdown_response = "処理が完了いたしました。下記リンクより結果ファイルをダウンロード願います。\n"
                        markdown_response += f"[{'結果ファイル'}]({file_url})"
                        
                        # Return final result in correct format
                        yield {"step": "final_answer", "message": markdown_response}
                    else:
                        print(f"Warning: Generated file does not exist at path: {full_file_path}")
                        yield {"step": "error", "message": f"File generation failed: Cannot find file at {full_file_path}"}
                elif doc_path:
                    yield {"step": "error", "message": f"Document generation error: {doc_path}"}
            else:
                # Return case where final_check is not yes
                answer = llm_result.get("answer", "")
                final_check = llm_result.get("final_check", "unknown")
                
                # Return different answers based on final_check value
                if final_check == "no":
                    message = "申し訳ありませんが、ご質問に関連する情報が見つかりませんでした。テーブル名や列名を具体的に指定していただくか、別の質問をお試しください。"
                else:
                    message = answer if answer else "回答を生成できませんでした。"
                
                yield {"step": "final_answer", "message": message}
            
        except Exception as e:
            import traceback
            error_msg = f"Error in RAG service: {str(e)}\n{traceback.format_exc()}"
            print(error_msg)
            yield {"step": "error", "message": f"処理中にエラーが発生しました:{str(e)}"}

    async def _generate_document(self, uuid: str) -> str:
        """Generate documents including Excel and PPT, and return filename"""
        try:
            print(f"Generating document for uuid: {uuid}")
            # Generate unique filename prefix
            filename_prefix = f"doc_{uuid}"
            
            # Get stored retrieval results from Redis
            neo4j_data = self.redis_tools.get(f"{uuid}:neo4j")
            opensearch_data = self.redis_tools.get(f"{uuid}:opensearch")
            postgresql_data = self.redis_tools.get(f"{uuid}:postgresql")
            
            # Log retrieved data
            print(f"Retrieved data from Redis - Neo4j: {type(neo4j_data)}, OpenSearch: {type(opensearch_data)}, PostgreSQL: {type(postgresql_data)}")
            if postgresql_data:
                if isinstance(postgresql_data, list) and len(postgresql_data) > 0:
                    print(f"PostgreSQL data sample: {postgresql_data[0]}")
                elif isinstance(postgresql_data, str):
                    print(f"PostgreSQL data (string): {postgresql_data[:100]}...")
            
            # Create document save directory
            os.makedirs(self.ppt_service.output_dir, exist_ok=True)
            
            # Initialize path variables
            ppt_path = ""
            
            # 1. First try to create relationship diagram PPT using Neo4j data
            if neo4j_data:
                print("Creating diagram PPT using Neo4j data")
                try:
                    # Ensure neo4j_data is in list format
                    if isinstance(neo4j_data, str):
                        try:
                            neo4j_data = json.loads(neo4j_data)
                        except:
                            neo4j_data = [{"content": neo4j_data, "source": "neo4j"}]
                    
                    if not isinstance(neo4j_data, list):
                        neo4j_data = [neo4j_data]
                    
                    # Ensure data is a list of dictionaries
                    processed_data = []
                    for item in neo4j_data:
                        if isinstance(item, dict):
                            processed_data.append(item)
                        else:
                            processed_data.append({"content": str(item), "score": 0.9, "source": "neo4j"})
                    
                    # 1.1 Generate Excel file
                    excel_path = await self.excel_service.export_to_excel(processed_data, filename_prefix)
                    
                    # 1.2 Use create_ppt function to create PPT with relationship diagram
                    ppt_path = os.path.join(self.ppt_service.output_dir, f"{filename_prefix}.pptx")
                    success = await self.ppt_service.create_ppt(excel_file=excel_path, output_file=ppt_path)
                    
                    # Check if PPT creation was successful
                    if not success or not os.path.exists(ppt_path):
                        print(f"Warning: Failed to create diagram PPT, falling back to regular PPT creation")
                        ppt_path = await self.ppt_service.export_to_ppt(excel_path, filename_prefix)
                except Exception as e:
                    print(f"Error creating Neo4j diagram PPT: {str(e)}")
                    neo4j_data = None  # Reset to None so we can try other data sources
            
            # If PPT wasn't created successfully, try other data sources
            if not ppt_path or not os.path.exists(ppt_path):
                print("No PPT created from Neo4j data, trying other data sources")
                # Try to create basic PPT using OpenSearch or PostgreSQL data
                for data_source, data in [("opensearch", opensearch_data), ("postgresql", postgresql_data)]:
                    if data:
                        try:
                            processed_data = data
                            if isinstance(data, str):
                                try:
                                    processed_data = json.loads(data)
                                except:
                                    processed_data = [{"content": data, "source": data_source}]
                            
                            if not isinstance(processed_data, list):
                                processed_data = [processed_data]
                            
                            # Format data as list of dictionaries
                            formatted_data = []
                            for item in processed_data:
                                if isinstance(item, dict):
                                    formatted_data.append(item)
                                else:
                                    formatted_data.append({"content": str(item), "source": data_source})
                            
                            # Generate Excel
                            excel_path = await self.excel_service.export_to_excel(formatted_data, filename_prefix)
                            
                            # Create PPT using regular method
                            ppt_path = await self.ppt_service.export_to_ppt(excel_path, filename_prefix)
                            
                            if ppt_path and os.path.exists(ppt_path):
                                print(f"Successfully created PPT from {data_source} data")
                                break
                        except Exception as e:
                            print(f"Error creating PPT from {data_source} data: {str(e)}")
                            continue
            
            # 2. If PPT was created, try to append OpenSearch data (one page per item)
            if ppt_path and os.path.exists(ppt_path) and opensearch_data:
                try:
                    if isinstance(opensearch_data, str):
                        try:
                            opensearch_data = json.loads(opensearch_data)
                        except:
                            opensearch_data = [{"content": opensearch_data, "source": "opensearch"}]
                    
                    if not isinstance(opensearch_data, list):
                        opensearch_data = [opensearch_data]
                    
                    # Ensure each item goes on a separate page
                    for item in opensearch_data:
                        if isinstance(item, dict):
                            await self.ppt_service.append_to_ppt([item], ppt_path)
                        else:
                            await self.ppt_service.append_to_ppt([{"content": str(item), "source": "opensearch"}], ppt_path)
                except Exception as e:
                    print(f"Error appending OpenSearch data to PPT: {str(e)}")
                    print(traceback.format_exc())
            
            # 3. If PPT was created, try to append PostgreSQL data (ensure content isn't truncated)
            if ppt_path and os.path.exists(ppt_path) and postgresql_data:
                try:
                    print("Adding PostgreSQL data to PPT")
                    if isinstance(postgresql_data, str):
                        try:
                            postgresql_data = json.loads(postgresql_data)
                        except:
                            postgresql_data = [{"content": postgresql_data, "source": "postgresql"}]
                    
                    if not isinstance(postgresql_data, list):
                        postgresql_data = [postgresql_data]
                    
                    # Collect all PostgreSQL content
                    postgresql_content = []
                    for item in postgresql_data:
                        if isinstance(item, dict):
                            postgresql_content.append(item)
                        else:
                            postgresql_content.append({"content": str(item), "source": "postgresql"})
                    
                    # Add PostgreSQL data to PPT, handling long content
                    # Check content length, may need multiple pages
                    for item in postgresql_content:
                        content = item.get("content", "")
                        if isinstance(content, str) and len(content) > 1000:
                            # Split long content into parts and add separately
                            chunks = [content[i:i+1000] for i in range(0, len(content), 1000)]
                            for i, chunk in enumerate(chunks):
                                chunk_item = item.copy()
                                chunk_item["content"] = chunk
                                chunk_item["title"] = f"PostgreSQL Data (Part {i+1}/{len(chunks)})"
                                await self.ppt_service.append_to_ppt([chunk_item], ppt_path)
                        else:
                            # Add short content directly
                            await self.ppt_service.append_to_ppt([item], ppt_path)
                except Exception as e:
                    print(f"Error appending PostgreSQL data to PPT: {str(e)}")
                    print(traceback.format_exc())
            
            # Return result
            if ppt_path and os.path.exists(ppt_path):
                yield os.path.basename(ppt_path)
            else:
                yield "Error: PPT file was not created"
                
        except Exception as e:
            import traceback
            error_msg = f"Error generating document: {str(e)}\n{traceback.format_exc()}"
            print(error_msg)
            yield f"Error: {str(e)}"

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