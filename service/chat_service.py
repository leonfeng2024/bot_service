from utils.singleton import singleton
from service.llm_service import LLMService
from service.rag_service import RAGService

@singleton
class ChatService:
    def __init__(self):
        self.bot_name = "bot"
        self.llm_service = LLMService()
        self.llm_service.init_llm("azure-gpt4")
        self.rag_service = RAGService()
    
    async def handle_chat(self, username: str, query: str) -> dict:
        try:
            # 获取相关文档
            docs = await self.rag_service.retrieve(query)
            
            # 构建prompt
            prompt = f"""以下のナレッジベースの内容に基づいてユーザーの質問に回答してください。  
提供されたナレッジベースの内容のみを使用して回答し、ナレッジベースに関連情報がない場合は、  
「申し訳ありませんが、関連情報は見つかりませんでした。」と返信してください。  

ナレッジベースの内容：
{chr(10).join([f'- {doc}' for doc in docs])}

ユーザーの質問：{query}

上記のナレッジベースの内容に基づき、簡潔かつ正確に回答してください。重複する内容は省いてください。
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