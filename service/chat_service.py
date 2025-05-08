from utils.singleton import singleton
from service.llm_service import LLMService
from service.rag_service import RAGService
from tools.redis_tools import RedisTools
from tools.postgresql_tools import PostgreSQLTools
import json
import traceback
import datetime

@singleton
class ChatService:
    def __init__(self):
        self.bot_name = "bot"
        self.llm_service = LLMService()
        # Don't specify LLM type, let system use global configuration
        self.llm_service.init_llm()
        self.rag_service = RAGService()
        self.redis_tools = RedisTools()
        self.postgresql_tools = PostgreSQLTools()
    
    async def _analyze_user_intent(self, query: str) -> dict:
        """
        Analysis User Intent
        """
        intent_prompt = """
Task Description
You are a semantic analysis expert specializing in analyzing users' text messages. You support text content in both English and Japanese, and you must strictly adhere to the following rules when conducting semantic analysis.
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
            # Call LLM to analyze user intent
            llm = self.llm_service.get_llm()
            response = await llm.generate(f"{intent_prompt}\n\n用户输入：{query}")
            
            # Clean response text, ensure it only contains JSON
            response = response.strip()
            if response.startswith('```json'):
                response = response[7:]
            if response.endswith('```'):
                response = response[:-3]
            response = response.strip()
            
            # Parse JSON response
            intent_result = json.loads(response)
            return intent_result
            
        except Exception as e:
            return {"category": "unknown", "message": "NG"}
    
    async def handle_chat(self, username: str, query: str, uuid: str = None) -> dict:
        try:
            # Analyze user intent
            try:
                intent_result = await self._analyze_user_intent(query)
                # Return intent recognition result
                yield {"step": "_analyze_user_intent", "message": "Semantic recognition completed"}
            except Exception as e:
                intent_result = {"category": "unknown", "message": "NG"}
                
            # Return message based on intent
            if intent_result["category"] == "unknown":
                # First return prompt message
                yield {
                    "status": "success",
                    "username": self.bot_name,
                    "message": "We provide a database table change dependency investigation service. Please let us know the table field name you want to change."
                }
                
                # Add final_answer step
                yield {
                    "step": "final_answer",
                    "message": "We provide a database table change dependency investigation service. Please let us know the table field name you want to change.",
                    "type": "text"
                }
                
                # Save system reply to chat history
                self._save_chat_history(username, uuid, "We provide a database table change dependency investigation service. Please let us know the table field name you want to change.", "bot")
                return
            
            # Identify fields
            try:
                column_results = {}
                async for result in self.llm_service.identify_column(query):
                    if isinstance(result, dict):
                        if "step" in result:
                            yield result
                        else:
                            column_results = result
                
                # Return the results if we have them
                if column_results:
                    yield {"step": "identify_column", "message": "Field identification successful", "data": column_results}
            except Exception as e:
                print(f"Error identifying columns: {str(e)}")
            
            # Get relevant documents
            try:
                # Save user query to chat_history table
                self._save_chat_history(username, uuid, query, "user")
                
                # Flag to track if final answer has been processed
                final_answer_processed = False
                
                # Get retrieval results from RAG service
                async for chunk in self.rag_service.retrieve(query, uuid):
                    # Directly pass intermediate status updates
                    if chunk.get("step") != "final_answer":
                        yield chunk
                        continue
                    
                    # Prevent duplicate final_answer
                    if final_answer_processed:
                        continue
                        
                    final_answer_processed = True
                    
                    # Process final answer
                    answer_message = chunk.get("message", "")
                    
                    # Get cache content from Redis and save as bot reply
                    bot_message = answer_message
                    self._save_chat_history(username, uuid, bot_message, "bot")
                    
                    # Return final answer
                    yield chunk
                
            except Exception as e:
                error_traceback = traceback.format_exc()
                # Return detailed error message for development environment
                detailed_error_message = f"Error type: {type(e).__name__}\nError message: {str(e)}\n\nTraceback:\n{error_traceback}"
                
                error_response = {
                    "step": "error",
                    "message": f"Error: {str(e)}"
                }
                
                # Log error message
                print(detailed_error_message)
                
                # Return simplified error message to client
                yield error_response
                
        except Exception as e:
            error_traceback = traceback.format_exc()
            # Return detailed error message for development environment
            detailed_error_message = f"Error type: {type(e).__name__}\nError message: {str(e)}\n\nTraceback:\n{error_traceback}"
            
            # Log error message
            print(detailed_error_message)
            
            # Return unified error message
            yield {
                "step": "error",
                "message": f"Error: {str(e)}"
            }
    
    def _get_redis_cache_content(self, uuid: str) -> str:
        """Get cached postgresql, neo4j and opensearch content from Redis"""
        try:
            content = ""
            data_sources = ['postgresql', 'neo4j', 'opensearch']
            
            for source in data_sources:
                source_data = self.redis_tools.get(f"{uuid}:{source}")
                if source_data:
                    content_json = json.dumps(source_data) if isinstance(source_data, (list, dict)) else str(source_data)
                    content += f"{source} data: {content_json}\n\n"
            
            return content if content else "No cache data found"
        except Exception as e:
            return f"Error retrieving cache data: {str(e)}"
    
    def _save_chat_history(self, username: str, uuid: str, message: str, sender: str) -> None:
        """Save chat history to chat_history table"""
        try:
            query = """
            INSERT INTO chat_history 
            (username, uuid, sender, message, createDate) 
            VALUES (%(username)s, %(uuid)s, %(sender)s, %(message)s, %(create_date)s)
            """
            
            parameters = {
                "username": username,
                "uuid": uuid,
                "sender": sender,
                "message": message,
                "create_date": datetime.datetime.now()
            }
            
            self.postgresql_tools.execute_query(query, parameters)
        except Exception as e:
            print(f"Error saving chat history: {str(e)}")
            print(traceback.format_exc())
    
    async def logout(self, username: str, uuid: str) -> dict:
        try:
            # Delete session data from Redis
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