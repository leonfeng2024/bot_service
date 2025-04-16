import asyncio
import os
from service.llm_service import LLMService
import config

async def test_llm():
    # 打印配置信息
    print(f"OPENAI_API_KEY: '{config.OPENAI_API_KEY[:10]}...{config.OPENAI_API_KEY[-5:]}'")
    print(f"OPENAI_PROJECT_ID: '{config.OPENAI_PROJECT_ID}'")
    print(f"OPENAI_MODEL_NAME: '{config.OPENAI_MODEL_NAME}'")
    
    # 测试 LLM 服务
    try:
        llm_service = LLMService()
        llm = llm_service.init_llm("openai-gpt41")
        
        # 测试生成
        response = await llm.generate("你好，请介绍一下你自己。")
        print(f"LLM 响应: {response}")
        
        # 测试 agent_llm
        agent_llm = llm_service.init_agent_llm("openai-gpt41")
        print(f"Agent LLM initialized: {agent_llm}")
        
    except Exception as e:
        import traceback
        print(f"Error: {str(e)}")
        print(f"Detailed error: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(test_llm()) 