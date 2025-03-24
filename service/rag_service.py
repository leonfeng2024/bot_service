from typing import List, Dict, Any, Optional
from utils.singleton import singleton
from service.llm_service import LLMService
from service.export_excel_service import ExportExcelService
from service.export_ppt_service import ExportPPTService
from tools.redis_tools import RedisTools
import json
import sys
import os
import asyncio

# Add project root to Python path to ensure Retriever can be imported
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import retrievers from new structure with absolute imports
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
        # Initialize LLM service
        self.llm_service = LLMService()
        # 初始化 Azure GPT-4 LLM
        self.llm_service.init_llm("azure-gpt4")
        # 使用RedisTools而不是直接创建Redis客户端
        self.redis_tools = RedisTools()
        # Initialize export services
        self.excel_service = ExportExcelService()
        self.ppt_service = ExportPPTService()
        # Initialize retrievers will be done in retrieve method
        pass
    
    async def _multi_source_retrieve(self, query: str, uuid: str = None) -> List[str]:
        """
        从多个来源检索数据
        返回格式为字符串列表：["Doc#1: content1", "Doc#2: content2", ...]
        """
        # Initialize retrievers
        retrievers = {
            'opensearch': OpenSearchRetriever(),
            'postgresql': PostgreSQLRetriever(),
            'neo4j': Neo4jRetriever()
        }
        
        # 用于JSON格式的中间结果存储，便于存入Redis
        json_results = []
        # 用于返回的字符串列表结果
        results = []
        # 文档编号计数器
        doc_counter = 1
        
        for source, retriever in retrievers.items():
            try:
                # 传递uuid参数给所有检索器
                source_results = await retriever.retrieve(query, uuid)
                
                if source_results and isinstance(source_results, list):
                    # 处理每个结果，转换为字符串格式并添加到结果列表
                    for result in source_results:
                        if isinstance(result, dict) and 'content' in result:
                            # 确保内容是字符串
                            content = result['content']
                            if not isinstance(content, str):
                                content = str(content)
                            
                            # 构造格式化的文档字符串
                            doc_string = f"Doc#{doc_counter}: {content}"
                            results.append(doc_string)
                            
                            # 同时保留JSON格式用于存储到Redis
                            result['source'] = source
                            json_results.append(result)
                            
                            # 增加文档计数器
                            doc_counter += 1
                else:
                    # 添加占位结果
                    error_content = f"从 {source} 检索的结果格式不正确"
                    results.append(f"Doc#{doc_counter}: {error_content}")
                    
                    # 同时保留JSON格式用于存储到Redis
                    json_results.append({
                        "content": error_content,
                        "score": 0.5,
                        "source": source
                    })
                    
                    # 增加文档计数器
                    doc_counter += 1
            except Exception as e:
                import traceback
                print(f"Error retrieving from {source}: {str(e)}")
                print(f"Detailed error: {traceback.format_exc()}")
                
                # 添加错误信息到结果
                error_content = f"从 {source} 检索数据时出错: {str(e)}"
                results.append(f"Doc#{doc_counter}: {error_content}")
                
                # 同时保留JSON格式用于存储到Redis
                json_results.append({
                    "content": error_content,
                    "score": 0.4,
                    "source": source
                })
                
                # 增加文档计数器
                doc_counter += 1
        
        # 如果没有任何结果，添加默认消息
        if not results:
            default_content = "未能从任何数据源检索到相关信息"
            results.append(f"Doc#1: {default_content}")
            
            # 同时保留JSON格式用于存储到Redis
            json_results.append({
                "content": default_content,
                "score": 0.3,
                "source": "system"
            })
        
        # 将JSON格式结果存储到Redis，便于后续使用
        if uuid:
            await self._store_in_redis(json_results, uuid)
            
        return results

    async def _rerank(self, results: List[Dict[str, Any]], query: str) -> List[Dict[str, Any]]:
        # Sort results by score field in descending order
        ranked_results = sorted(results, key=lambda x: x['score'], reverse=True)
        return ranked_results

    async def _process_with_llm(self, docs: List[str], query: str) -> dict:
        """使用LLM处理检索结果"""
        try:
            # 构建prompt
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
            
            # 使用LLM生成回答
            llm = self.llm_service.get_llm()
            response = await llm.generate(prompt)
            
            # 清理响应，提取"yes"或"no"
            cleaned_response = response.strip().lower()
            # 移除引号和其他格式符号
            cleaned_response = cleaned_response.replace('"', '').replace("'", '')
            cleaned_response = cleaned_response.split('\n')[0]  # 只保留第一行
            
            # 判断是yes还是no
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
            print(f"Error processing with LLM: {str(e)}")
            return {
                "final_check": "unknown",
                "answer": f"Error processing your query: {str(e)}"
            }


    async def _generate_excel_and_ppt(self, query: str, uuid: str) -> str:
        """
        Generate Excel and PPT files based on Redis data
        Returns the PPT filename
        """
        try:
            # 强制创建output目录
            os.makedirs(self.ppt_service.output_dir, exist_ok=True)
            print(f"Output directory ensured: {self.ppt_service.output_dir}")
            
            # Get Neo4j data from Redis
            neo4j_data = self.redis_tools.get(f"{uuid}:neo4j")
            print(f"Neo4j data from Redis: {neo4j_data}")
            
            # 尝试获取任何可用的数据
            data_to_use = None
            data_source = None
            
            if neo4j_data and isinstance(neo4j_data, list) and len(neo4j_data) > 0:
                data_to_use = neo4j_data
                data_source = "neo4j"
            else:
                # 尝试OpenSearch数据
                opensearch_data = self.redis_tools.get(f"{uuid}:opensearch")
                print(f"OpenSearch data: {opensearch_data}")
                if opensearch_data and isinstance(opensearch_data, list) and len(opensearch_data) > 0:
                    data_to_use = opensearch_data
                    data_source = "opensearch"
                else:
                    # 尝试PostgreSQL数据
                    postgresql_data = self.redis_tools.get(f"{uuid}:postgresql")
                    print(f"PostgreSQL data: {postgresql_data}")
                    if postgresql_data and isinstance(postgresql_data, list) and len(postgresql_data) > 0:
                        data_to_use = postgresql_data
                        data_source = "postgresql"
            
            # 如果有数据可用，生成文件
            if data_to_use:
                print(f"Using data from {data_source} with {len(data_to_use)} items")
                
                # 创建一个临时的简单数据集，确保能生成文件
                if len(data_to_use) == 0 or not isinstance(data_to_use[0], dict):
                    print("Creating fallback data since data format is invalid")
                    data_to_use = [
                        {"content": "Generated dummy data 1", "score": 0.95, "source": data_source},
                        {"content": "Generated dummy data 2", "score": 0.90, "source": data_source}
                    ]
                
                # Excel文件使用完整路径
                filename_prefix = f"{uuid}_{data_source}"
                excel_path = os.path.join(self.ppt_service.output_dir, f"{filename_prefix}.xlsx")
                print(f"Will create Excel file at: {excel_path}")
                
                # 导出到Excel
                excel_file = await self.excel_service.export_to_excel(data_to_use, filename_prefix)
                print(f"Excel file created: {excel_file}")
                
                # 确认Excel文件已创建
                if not os.path.exists(excel_file):
                    print(f"WARNING: Excel file was not created at {excel_file}")
                    # 尝试直接使用pandas生成Excel
                    import pandas as pd
                    df = pd.DataFrame(data_to_use)
                    excel_path = os.path.join(self.ppt_service.output_dir, f"{filename_prefix}.xlsx")
                    df.to_excel(excel_path, index=False)
                    excel_file = excel_path
                    print(f"Directly created Excel file: {excel_file}")
                
                # 导出Excel数据到PPT
                ppt_file = await self.ppt_service.export_to_ppt(excel_file, filename_prefix)
                print(f"PPT file created: {ppt_file}")
                
                # 确认PPT文件已创建
                if not os.path.exists(ppt_file):
                    print(f"WARNING: PPT file was not created at {ppt_file}")
                    return "Error: PPT file was not created"
                
                # 如果有其他数据源的数据，也添加到PPT中
                if data_source != "opensearch":
                    opensearch_data = self.redis_tools.get(f"{uuid}:opensearch")
                    if opensearch_data and isinstance(opensearch_data, list) and len(opensearch_data) > 0:
                        print(f"Adding OpenSearch data to PPT ({len(opensearch_data)} items)")
                        try:
                            ppt_file = await self.ppt_service.append_to_ppt(opensearch_data, ppt_file)
                            print(f"PPT file updated with OpenSearch data: {ppt_file}")
                        except Exception as append_error:
                            print(f"Error appending OpenSearch data: {str(append_error)}")
                
                # 完整路径
                full_path = os.path.abspath(ppt_file)
                print(f"Final PPT file path: {full_path}")
                
                # 返回文件名
                return os.path.basename(ppt_file)
            else:
                print("No data available to generate documents - all data sources returned empty results")
                return "No data available to generate documents"
                
        except Exception as e:
            import traceback
            print(f"Error generating Excel and PPT: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            return f"Error generating document: {str(e)}"

    async def _store_in_redis(self, results: List[Dict[str, Any]], uuid: str):
        """Store retrieval results in Redis by source"""
        try:
            # Group results by source
            grouped_results = {}
            for result in results:
                source = result.get('source', 'unknown')
                if source not in grouped_results:
                    grouped_results[source] = []
                grouped_results[source].append(result)
                
            # Store each source's results in Redis
            for source, source_results in grouped_results.items():
                try:
                    key = f"{uuid}:{source}"
                    # 使用RedisTools的set方法，它会自动处理过期时间
                    # RedisTools.set包含了设置过期时间的功能
                    self.redis_tools.set(key, source_results)
                    print(f"Successfully stored {source} results in Redis with key: {key}")
                except Exception as source_error:
                    print(f"Error storing {source} results in Redis: {str(source_error)}")
                    # Continue with other sources
        except Exception as e:
            import traceback
            print(f"Error in _store_in_redis: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            # Don't re-raise the exception to prevent the whole chain from failing

    async def retrieve(self, query: str, uuid: str):
        """
        从多个数据源检索结果，处理后返回
        final_check: 如果为"yes"，生成excel和ppt文件
        """
        print(f"Starting RAG process for query: {query}, uuid: {uuid}")
        
        # 从多个数据源检索结果（新格式：字符串列表）
        results = await self._multi_source_retrieve(query, uuid)
        print("Retrieved documents:")
        for doc in results[:3]:  # 只打印前3条记录，避免日志过长
            print(f"  {doc[:100]}..." if len(doc) > 100 else f"  {doc}")
        print(f"  ... (total {len(results)} documents)")
        
        if not results:
            return {"status": "error", "message": "No results found"}
        
        # 直接处理字符串列表格式的结果，不需要重新排序
        llm_response = await self._process_with_llm(results, query)
        # 检查是否需要生成excel和ppt
        doc_path = None
        if llm_response['final_check'] == "yes":
            print(f"final_check is 'yes', generating Excel and PPT files for uuid: {uuid}")
            # 使用uuid从Redis获取之前存储的JSON格式结果生成文件
            doc_path = await self._generate_excel_and_ppt(query, uuid)
            print(f"Document generation result: {doc_path}")

        # 准备响应
        response = {
            "status": "success",
            "message": llm_response
        }

        # 添加文件信息（如果生成了文件）
        if doc_path and doc_path not in ["No data available to generate documents", "Error: PPT file was not created"] and not doc_path.startswith("Error"):
            print(f"Adding document path to response: {doc_path}")
            output_dir = os.path.abspath(self.ppt_service.output_dir)
            full_file_path = os.path.join(output_dir, doc_path)
            
            # 检查文件是否存在
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
            # 如果有错误或没有数据
            response["error"] = doc_path
            print(f"Error in document generation: {doc_path}")
        
        return response