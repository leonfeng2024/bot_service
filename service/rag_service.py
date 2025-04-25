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
        # 不指定特定LLM类型，让系统自动使用全局配置
        self.llm_service.init_llm()
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
                
                # 返回每个检索器的完成状态
                yield {"step": f"{source}_retriever", "message": f"{source.capitalize()}数据库查询完成"}
                
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
            
            # 返回LLM处理结果
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
        """检索相关文档并生成回答"""
        try:
            print(f"Starting RAG process for query: {query}, uuid: {uuid}")
            
            # 返回处理开始信息
            yield {"step": "process_start", "message": "开始处理查询"}
            
            # 获取文档
            docs = []
            async for doc_batch in self._multi_source_retrieve(query, uuid):
                # Check if this is a status update
                if isinstance(doc_batch, dict) and "step" in doc_batch:
                    yield doc_batch
                    continue
                docs = doc_batch
            
            # 返回文档检索完成状态
            yield {"step": "docs_retrieved", "message": f"检索到 {len(docs)} 个文档"}
            
            print("Retrieved documents:")
            for doc in docs[:3]:
                print(f"  {doc[:100]}..." if len(doc) > 100 else f"  {doc}")
            print(f"  ... (total {len(docs)} documents)")
            
            # 使用LLM处理文档
            llm_result = None
            async for result in self._process_with_llm(docs, query):
                if isinstance(result, dict):
                    if "step" in result:
                        yield result
                    else:
                        llm_result = result
            
            if not llm_result:
                llm_result = {"final_check": "unknown", "answer": "Failed to process query"}
            
            # 返回LLM处理完成状态
            yield {"step": "llm_process_complete", "message": "LLM处理完成"}
            
            # 获取token使用情况
            token_usage = self.llm_service.get_formatted_token_usage()
            
            # 如果final_check是yes，则生成Excel和PPT文档
            if llm_result.get("final_check") == "yes" and uuid:
                # 返回开始生成文档的状态
                yield {"step": "generating_document", "message": "正在生成文档..."}
                
                print(f"final_check is 'yes', generating Excel and PPT files for uuid: {uuid}")
                doc_path = None
                async for path in self._generate_document(uuid):
                    doc_path = path
                print(f"Document generation result: {doc_path}")
                
                # 如果生成了文档，添加文档信息到响应中
                if doc_path and doc_path not in ["No data available to generate documents", "Error: PPT file was not created"] and not doc_path.startswith("Error"):
                    print(f"Adding document path to response: {doc_path}")
                    output_dir = os.path.abspath(self.ppt_service.output_dir)
                    full_file_path = os.path.join(output_dir, doc_path)
                    
                    if os.path.exists(full_file_path):
                        # 生成markdown格式的链接
                        file_name = os.path.basename(full_file_path)
                        file_url = f"http://localhost:8088/output/{file_name}"
                        markdown_response = "処理が完了いたしました。下記リンクより結果ファイルをダウンロード願います。\n"
                        markdown_response += f"[{'結果ファイル'}]({file_url})"
                        
                        # 返回最终结果，使用正确的格式
                        yield {"step": "final_answer", "message": markdown_response}
                    else:
                        print(f"Warning: Generated file does not exist at path: {full_file_path}")
                        yield {"step": "error", "message": f"文件生成失败: 无法在 {full_file_path} 找到文件"}
                elif doc_path:
                    yield {"step": "error", "message": f"文档生成出错: {doc_path}"}
            else:
                # 返回final_check不是yes的情况
                answer = llm_result.get("answer", "")
                final_check = llm_result.get("final_check", "unknown")
                
                # 根据final_check的值返回不同的回答
                if final_check == "no":
                    message = "申し訳ありませんが、ご質問に関連する情報が見つかりませんでした。テーブル名や列名を具体的に指定していただくか、別の質問をお試しください。"
                else:
                    message = answer if answer else "回答を生成できませんでした。"
                
                yield {"step": "final_answer", "message": message}
            
        except Exception as e:
            import traceback
            error_msg = f"Error in RAG service: {str(e)}\n{traceback.format_exc()}"
            print(error_msg)
            yield {"step": "error", "message": f"処理中にエラーが発生しました: {str(e)}"}

    async def _generate_document(self, uuid: str) -> str:
        """生成文档包括Excel和PPT，并返回文件名"""
        try:
            print(f"Generating document for uuid: {uuid}")
            # 生成唯一文件名前缀
            filename_prefix = f"doc_{uuid}"
            
            # 从Redis获取存储的检索结果
            neo4j_data = self.redis_tools.get(f"{uuid}:neo4j")
            opensearch_data = self.redis_tools.get(f"{uuid}:opensearch")
            postgresql_data = self.redis_tools.get(f"{uuid}:postgresql")
            
            # 记录获取到的数据
            print(f"Retrieved data from Redis - Neo4j: {type(neo4j_data)}, OpenSearch: {type(opensearch_data)}, PostgreSQL: {type(postgresql_data)}")
            if postgresql_data:
                if isinstance(postgresql_data, list) and len(postgresql_data) > 0:
                    print(f"PostgreSQL data sample: {postgresql_data[0]}")
                elif isinstance(postgresql_data, str):
                    print(f"PostgreSQL data (string): {postgresql_data[:100]}...")
            
            # 创建文档保存目录
            os.makedirs(self.ppt_service.output_dir, exist_ok=True)
            
            # 路径变量初始化
            ppt_path = ""
            
            # 1. 首先尝试用Neo4j数据创建关系图PPT
            if neo4j_data:
                print("Creating diagram PPT using Neo4j data")
                try:
                    # 确保neo4j_data是列表格式
                    if isinstance(neo4j_data, str):
                        try:
                            neo4j_data = json.loads(neo4j_data)
                        except:
                            neo4j_data = [{"content": neo4j_data, "source": "neo4j"}]
                    
                    if not isinstance(neo4j_data, list):
                        neo4j_data = [neo4j_data]
                    
                    # 确保数据是一个字典列表
                    processed_data = []
                    for item in neo4j_data:
                        if isinstance(item, dict):
                            processed_data.append(item)
                        else:
                            processed_data.append({"content": str(item), "score": 0.9, "source": "neo4j"})
                    
                    # 1.1 生成Excel文件
                    excel_path = await self.excel_service.export_to_excel(processed_data, filename_prefix)
                    
                    # 1.2 使用create_ppt函数创建包含关系图的PPT
                    ppt_path = os.path.join(self.ppt_service.output_dir, f"{filename_prefix}.pptx")
                    success = await self.ppt_service.create_ppt(excel_file=excel_path, output_file=ppt_path)
                    
                    # 检查PPT创建是否成功
                    if not success or not os.path.exists(ppt_path):
                        print(f"Warning: Failed to create diagram PPT, falling back to regular PPT creation")
                        ppt_path = await self.ppt_service.export_to_ppt(excel_path, filename_prefix)
                except Exception as e:
                    print(f"Error creating Neo4j diagram PPT: {str(e)}")
                    neo4j_data = None  # Reset to None so we can try other data sources
            
            # 如果没有成功创建PPT，尝试使用其他数据源
            if not ppt_path or not os.path.exists(ppt_path):
                print("No PPT created from Neo4j data, trying other data sources")
                # 尝试使用OpenSearch或PostgreSQL数据创建基本PPT
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
                            
                            # 将数据格式化为字典列表
                            formatted_data = []
                            for item in processed_data:
                                if isinstance(item, dict):
                                    formatted_data.append(item)
                                else:
                                    formatted_data.append({"content": str(item), "source": data_source})
                            
                            # 生成Excel
                            excel_path = await self.excel_service.export_to_excel(formatted_data, filename_prefix)
                            
                            # 使用常规方法创建PPT
                            ppt_path = await self.ppt_service.export_to_ppt(excel_path, filename_prefix)
                            
                            if ppt_path and os.path.exists(ppt_path):
                                print(f"Successfully created PPT from {data_source} data")
                                break
                        except Exception as e:
                            print(f"Error creating PPT from {data_source} data: {str(e)}")
                            continue
            
            # 2. 如果已创建PPT，尝试追加OpenSearch数据（每项一页）
            if ppt_path and os.path.exists(ppt_path) and opensearch_data:
                try:
                    if isinstance(opensearch_data, str):
                        try:
                            opensearch_data = json.loads(opensearch_data)
                        except:
                            opensearch_data = [{"content": opensearch_data, "source": "opensearch"}]
                    
                    if not isinstance(opensearch_data, list):
                        opensearch_data = [opensearch_data]
                    
                    # 确保每项放在单独页面
                    for item in opensearch_data:
                        if isinstance(item, dict):
                            await self.ppt_service.append_to_ppt([item], ppt_path)
                        else:
                            await self.ppt_service.append_to_ppt([{"content": str(item), "source": "opensearch"}], ppt_path)
                except Exception as e:
                    print(f"Error appending OpenSearch data to PPT: {str(e)}")
                    print(traceback.format_exc())
            
            # 3. 如果已创建PPT，尝试追加PostgreSQL数据（确保内容不被截断）
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
                    
                    # 收集所有PostgreSQL内容
                    postgresql_content = []
                    for item in postgresql_data:
                        if isinstance(item, dict):
                            postgresql_content.append(item)
                        else:
                            postgresql_content.append({"content": str(item), "source": "postgresql"})
                    
                    # 将PostgreSQL数据添加到PPT，处理长内容
                    # 检查内容长度，可能需要多页
                    for item in postgresql_content:
                        content = item.get("content", "")
                        if isinstance(content, str) and len(content) > 1000:
                            # 长内容分成多个部分并分别添加
                            chunks = [content[i:i+1000] for i in range(0, len(content), 1000)]
                            for i, chunk in enumerate(chunks):
                                chunk_item = item.copy()
                                chunk_item["content"] = chunk
                                chunk_item["title"] = f"PostgreSQL Data (Part {i+1}/{len(chunks)})"
                                await self.ppt_service.append_to_ppt([chunk_item], ppt_path)
                        else:
                            # 短内容直接添加
                            await self.ppt_service.append_to_ppt([item], ppt_path)
                except Exception as e:
                    print(f"Error appending PostgreSQL data to PPT: {str(e)}")
                    print(traceback.format_exc())
            
            # 返回结果
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