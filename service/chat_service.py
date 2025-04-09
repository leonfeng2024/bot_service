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
            return intent_result
            
        except Exception as e:
            return {"category": "unknown", "message": "NG"}
    
    async def handle_chat(self, username: str, query: str, uuid: str = None) -> dict:
        try:
            # 分析用户意图
            try:
                # intent_result = await self._analyze_user_intent(query)
                intent_result = {"category":"schema_change", "message":"OK"}
            except Exception as e:
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
                
                # 构建响应消息
                final_check = retrieval_response.get("final_check", "unknown")
                if final_check == "yes":
                    message = f"処理が完了いたしました。下記リンクより結果ファイルをダウンロード願います。LINK"
                    return {
                        "status": "success",
                        "username": self.bot_name,
                        "message": message
                    }
                else:
                    message = f"処理が完了いたしました。関連のデータが見つかりませんでした"
                    return {
                        "status": "success",
                        "username": self.bot_name,
                        "message": message
                    }
                
            except Exception as e:
                error_traceback = traceback.format_exc()
                # 为开发环境返回详细错误信息
                detailed_error_message = f"Error type: {type(e).__name__}\nError message: {str(e)}\n\nTraceback:\n{error_traceback}"
                
                return {
                    "status": "failed",
                    "username": self.bot_name,
                    "message": f"Error: {str(e)}",
                    "debug_info": detailed_error_message
                }
                
        except Exception as e:
            error_traceback = traceback.format_exc()
            # 为开发环境返回详细错误信息
            detailed_error_message = f"Error type: {type(e).__name__}\nError message: {str(e)}\n\nTraceback:\n{error_traceback}"
            
            return {
                "status": "failed",
                "username": self.bot_name,
                "message": f"Error: {str(e)}",
                "debug_info": detailed_error_message
            }
    
    async def logout(self, username: str, uuid: str) -> dict:
        try:
            # 删除Redis中的会话数据
            self.redis_tools.delete(uuid)
            
            return {
                "status": "success",
                "message": "Logged out successfully"
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error during logout: {str(e)}"
            }