from utils.singleton import singleton
from service.llm_service import LLMService
from service.rag_service import RAGService
import json

@singleton
class ChatService:
    def __init__(self):
        self.bot_name = "bot"
        self.llm_service = LLMService()
        self.llm_service.init_llm("azure-gpt4")
        self.rag_service = RAGService()
    
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
User Input:
"I want to change a column from varchar(50) to varchar(100). Will this affect existing data?"
Response:
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
    
    async def handle_chat(self, username: str, query: str) -> dict:
        try:
            # 分析用户意图
            intent_result = await self._analyze_user_intent(query)
            print(intent_result)
            # 根据意图返回相应消息
            if intent_result["category"] == "unknown":
                return {
                    "status": "success",
                    "username": self.bot_name,
                    "message": "データベーステーブル変更の依存関係調査サービスを提供しております。変更対象のテーブルField名をお知らせください"
                }
            
            # 获取相关文档
            docs = await self.rag_service.retrieve(query)
            
            # 构建prompt
            prompt = f"""Answer user questions **strictly** based on the following knowledge base content. Only use provided documents to respond.
 
### Knowledge Base Content  
{chr(10).join([f'- Doc#{i+1}: {doc}' for i, doc in enumerate(docs)])}
 
### User Question  
{query}  
 
### Processing Rules  
1. Return **JSON format only**  
2. First determine if the user's question relates to the knowledge base:  
   - If **no relation**, return `"none"`  
   - If **answer exists**, return `"yes"`  
3. **Strictly prohibited**:  
   - Additional explanations beyond "none"/"yes"  
   - Using external knowledge beyond provided documents  
 
### Response Format  
```json 
{"category": "none" | "yes"}

Example
Knowledge Base:
Doc#1: Flight CA123 departs at 09:00 from Beijing
Doc#2: Flight MU456 arrives at 14:30 in Shanghai
User Question:
"Does flight CA123 have delay history?"

Response:
{"category": "none"}
"""
            
            # 调用LLM生成回答
            llm = self.llm_service.get_llm()
            response = await llm.generate(prompt)
            print(response)
            return {
                "status": "success",
                "username": self.bot_name,
                "message": response
            }
        except Exception as e:
            print(str(e))
            return {
                "status": "failed",
                "username": self.bot_name,
                "message": str(e)
            }