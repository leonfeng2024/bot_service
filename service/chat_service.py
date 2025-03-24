from utils.singleton import singleton
from service.llm_service import LLMService
from service.rag_service import RAGService
from tools.redis_tools import RedisTools
import json
import traceback

@singleton
class ChatService:
    def __init__(self):
        self.bot_name = "bot"
        self.llm_service = LLMService()
        self.llm_service.init_llm("azure-gpt4")
        self.rag_service = RAGService()
        self.redis_tools = RedisTools()
    
    async def _analyze_user_intent(self, query: str) -> dict:
        """
        Analysis User Intent
        """
        intent_prompt = """
Task Description
Analyze user input to determine if it's a database schema change impact inquiry, focusing on column/field modifications. Return classification results in JSON format.

Logic & Rules
1. Change Type Identification (Priority: High to Low)
Column Modification Keywords:
["column", "field", "add column", "drop column", "modify column", "rename column", "add constraint", "drop constraint", "alter column type", "adjust index"]
Schema Change Keywords:
["ALTER TABLE", "column deletion", "alter column type", "add constraint", "rename column", "adjust index", "schema change", "column adjustment"]
2. Technical Term Recognition
Identify database-specific terms:
["DDL operations", "schema version", "metadata change", "ONLINE DDL", "trigger invalidation", "data migration", "table design", "column definition"]

3. Contextual Analysis
Look for:

Operational descriptions:
"modify column length", "drop NOT NULL constraint", "add unique index", "change varchar(50) to varchar(100)"
Impact inquiries:
"Will this affect existing data?", "Does it require downtime?", "Is it backward compatible?"
4. Pattern Matching
Match these typical patterns:

"Will changing varchar(50) to varchar(100) affect existing data?"
"Will triggers become invalid after dropping a column?"
"Could adding a unique constraint cause duplicate errors?"
5. Exclusion Rules
Return unknown if:

Contains negatives: "not yet implemented", "test only", "no changes involved"
Discusses theories: "design patterns", "normalization theory", "database optimization"
Lacks both change operation AND impact inquiry
Response Format
// Schema change impact inquiry 
{"category":"schema_change", "message":"OK"}
// Non-schema change inquiry 
{"category":"unknown", "message":"NG"}

Examples
Example 1
User Input: "I want to change a column from varchar(50) to varchar(100). Will this affect existing data?"
User Input: "I want to rename a column in my table. Will this affect my existing queries?"  
User Input: "I need to change a column's data type from VARCHAR to INT. What should I consider?"  
User Input: "How can I set a default value for a column in my database table?"  
User Input: "I want to extend a column's length from VARCHAR(50) to VARCHAR(100). Will this impact my data?"  
User Input: "How do I add a NOT NULL constraint to an existing column?"  
User Input: "I need to delete a column that is no longer used. What steps should I take?"  
User Input: "How can I split a single column into multiple columns for better data management?"  
User Input: "Is it possible to merge two columns into one? How does it work?"  
User Input: "Can I reorder the columns in my table? Will it affect my application?"  
User Input: "How do I create a computed column based on other columns in my table?"  
User Input: "What is the process for adding an index to a column to improve query performance?"  
User Input: "How can I update the comment or description of a column in my table?"  
User Input: "I need to encrypt a sensitive column. What are the best practices?"  
User Input: "How do I add a foreign key constraint to a column in my table?"  
User Input: "Can I change the collation of a column to support multiple languages?"  
User Input: "How do I update the business logic of a column, such as changing its status values?"  
User Input: "What is the best way to add a validation rule to a column, like email format checking?"  
User Input: "I need to change the unit of a column from kg to lbs. How can I do this?"  
User Input: "How can I enable version control for a column to track its historical changes?"  
User Input: "What steps are required to modify the permissions of a column for better security?"
All above user input should Response:
{"category":"schema_change", "message":"OK"}

Example 2
User Input:
"How to apply Third Normal Form in database design?"
Response:
{"category":"unknown", "message":"NG"}

Example 3
User Input:
"Column changes haven't been executed yet, just testing."
Response:
{"category":"unknown", "message":"NG"}
"""

        try:
            # 调用LLM分析用户意图
            llm = self.llm_service.get_llm()
            response = await llm.generate(f"{intent_prompt}\n\n用户输入：{query}")
            
            # 清理响应文本，确保只包含JSON
            response = response.strip()
            if response.startswith('```json'):
                response = response[7:]
            if response.endswith('```'):
                response = response[:-3]
            response = response.strip()
            
            # 解析JSON响应
            intent_result = json.loads(response)
            print(f"Intent analysis result: {intent_result}")
            return intent_result
            
        except Exception as e:
            print(f"Error analyzing user intent: {str(e)}")
            print(f"Raw response: {response}")
            return {"category": "unknown", "message": "NG"}
    
    async def handle_chat(self, username: str, query: str, uuid: str = None) -> dict:
        try:
            # 分析用户意图
            try:
                intent_result = await self._analyze_user_intent(query)
                print(intent_result)
            except Exception as e:
                print(f"Error analyzing user intent: {str(e)}")
                intent_result = {"category": "unknown", "message": "NG"}
                
            # 根据意图返回相应消息
            if intent_result["category"] == "unknown":
                return {
                    "status": "success",
                    "username": self.bot_name,
                    "message": "データベーステーブル変更の依存関係調査サービスを提供しております。変更対象のテーブルField名をお知らせください"
                }
            
            # 获取相关文档
            try:
                # 从RAG服务获取检索结果
                retrieval_response = await self.rag_service.retrieve(query, uuid)
                
                # 检查返回状态
                if retrieval_response.get("status") != "success":
                    raise Exception(f"Error retrieving documents: {retrieval_response.get('error', 'Unknown error')}")
                
                # 由于修改了rag_service的返回格式，这里需要直接使用RAG的_multi_source_retrieve方法获取文档列表
                docs = await self.rag_service._multi_source_retrieve(query, uuid)
                
                # 构建prompt
                prompt = f"""Answer user questions **strictly** based on the following knowledge base content. Only use provided documents to respond.
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

### Example1
Example User Question:
"I want to change column [avg_file_size] of table documents_info "
Example Knowledge Base:
Doc#1: 表 GetEmployeeDetails 通过字段 employee_id 关联到表 employee_details 的字段 employee_id
Doc#2: 
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

Example Response:
"no"
Followed Reason 1: the columns in knowledge base are not as same as [avg_file_size].
Followed Reason 2: column [avg_file_size] is not exist in knowledge base.

### Example2
Example User Question:
"我想要修改employees字段"
Example Knowledge Base:
Doc#1: 表 p_UpdateEmployeeSalary 通过字段 changed_by 关联到表 employees 的字段 employee_id
Example Response:
"yes"
Followed Reason: column [employees] is exist in knowledge base.

### Knowledge Base Content  
{chr(10).join([f'- {doc}' for doc in docs])}
 
### User Question  
{query}  
"""
                
                # 调用LLM生成回答
                llm = self.llm_service.get_llm()
                response = await llm.generate(prompt)
                print(response)
                
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
                # TODO
                # 如果提供了UUID，将final_check保存到Redis
                if uuid:
                    try:
                        # 获取现有的缓存数据
                        cached_data = self.redis_tools.get(uuid) or {}
                        
                        # 添加final_check结果
                        cached_data["final_check"] = final_check
                        
                        # 更新Redis缓存
                        self.redis_tools.set(uuid, cached_data)
                        print(f"Updated final_check '{final_check}' for UUID: {uuid}")
                    except Exception as cache_error:
                        print(f"Error updating final_check in Redis: {str(cache_error)}")
                
                # 返回结果
                return {
                    "status": "success",
                    "username": self.bot_name,
                    "message": response
                }
                
            except Exception as retrieval_error:
                error_traceback = traceback.format_exc()
                print(f"Error during retrieval: {str(retrieval_error)}")
                print(f"Detailed error traceback: \n{error_traceback}")
                
                # 为开发环境返回详细错误信息
                detailed_error_message = f"Error type: {type(retrieval_error).__name__}\nError message: {str(retrieval_error)}\n\nTraceback:\n{error_traceback}"
                
                # 检查是否为认证错误
                if "Authentication required" in str(retrieval_error) or "Unauthorized" in str(retrieval_error):
                    error_details = "認証エラーが発生しました。データベース接続の認証情報を確認してください。"
                    error_details += f"\n\nTechnical details: {str(retrieval_error)}"
                else:
                    error_details = "申し訳ありませんが、データ取得中にエラーが発生しました。もう一度お試しください。"
                
                return {
                    "status": "error",
                    "username": self.bot_name,
                    "message": error_details,
                    "debug_info": detailed_error_message  # 添加调试信息字段
                }
                
        except Exception as e:
            error_traceback = traceback.format_exc()
            print(f"Unhandled error: {str(e)}")
            print(f"Detailed error traceback: \n{error_traceback}")
            
            # 为开发环境返回详细错误信息
            detailed_error_message = f"Error type: {type(e).__name__}\nError message: {str(e)}\n\nTraceback:\n{error_traceback}"
            
            return {
                "status": "failed",
                "username": self.bot_name,
                "message": "システムエラーが発生しました。サポートにお問い合わせください。",
                "debug_info": detailed_error_message  # 添加调试信息字段
            }