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
            prompt = f"""基于以下知识库内容回答用户问题。只能使用提供的知识库内容作答，如果知识库中没有相关信息，请直接回复'抱歉，我没有找到相关信息'。

知识库内容：
{chr(10).join([f'- {doc}' for doc in docs])}

用户问题：{query}

请基于以上知识库内容，简洁准确地回答问题，去除重复内容。
"""
            
            # 调用LLM生成回答
            llm = self.llm_service.get_llm()
            response = await llm.generate(prompt)
            
            return {
                "status": "success",
                "username": self.bot_name,
                "message": response
            }
        except Exception as e:
            return {
                "status": "failed",
                "username": self.bot_name,
                "message": str(e)
            }