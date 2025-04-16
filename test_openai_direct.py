import asyncio
from openai import OpenAI, AzureOpenAI
import config
import os
import sys
import time

def test_openai_direct():
    """
    直接测试OpenAI模型和备用机制，绕过服务层
    """
    try:
        # 打印配置信息
        print("="*50)
        print("测试配置")
        print("="*50)
        print(f"OPENAI_API_KEY: '{config.OPENAI_API_KEY[:5]}...{config.OPENAI_API_KEY[-4:]}'")
        print(f"OPENAI_PROJECT_ID: '{config.OPENAI_PROJECT_ID}'")
        print(f"OPENAI_MODEL_NAME: '{config.OPENAI_MODEL_NAME}'")
        print(f"AZURE_OPENAI_API_KEY: '{config.AZURE_OPENAI_API_KEY[:5]}...{config.AZURE_OPENAI_API_KEY[-4:]}'")
        print(f"AZURE_OPENAI_API_BASE: '{config.AZURE_OPENAI_API_BASE}'")
        print(f"AZURE_OPENAI_MODEL_NAME: '{config.AZURE_OPENAI_MODEL_NAME}'")
        print("="*50)

        # 测试1: 尝试列出OpenAI可用模型
        print("\n测试1: 检查OpenAI连接")
        print("-"*50)
        openai_available = True
        try:
            client = OpenAI(
                api_key=config.OPENAI_API_KEY,
                project=config.OPENAI_PROJECT_ID
            )
            print("尝试连接OpenAI API...")
            # 尝试列出模型 - 这将很快显示任何连接/认证问题
            models = client.models.list(limit=5)
            model_names = [model.id for model in models.data[:5]]
            print(f"成功连接OpenAI API并获取模型列表: {model_names}")
        except Exception as e:
            error_message = str(e)
            openai_available = False
            print(f"OpenAI API连接失败: {error_message}")
            if "403" in error_message and "unsupported_country_region_territory" in error_message:
                print("检测到区域限制错误 - 在中国大陆访问会受到限制")
            elif "401" in error_message:
                print("认证错误 - API密钥可能无效")
            elif "timeout" in error_message.lower():
                print("连接超时 - 可能需要代理")
            print("-"*50)

        # 测试2: 尝试生成GPT-4.1回复
        if openai_available:
            print("\n测试2: 尝试调用GPT-4.1")
            print("-"*50)
            try:
                print("尝试使用GPT-4.1生成回复...")
                completion = client.chat.completions.create(
                    model=config.OPENAI_MODEL_NAME,
                    messages=[{"role": "user", "content": "简短介绍一下你自己"}],
                    stream=False,
                    extra_headers={"OpenAI-Beta": "assistants=v1"}
                )
                content = completion.choices[0].message.content
                print(f"GPT-4.1响应成功: {content[:100]}...")
                print("-"*50)
            except Exception as e:
                error_message = str(e)
                print(f"GPT-4.1调用失败: {error_message}")
                print("即使连接成功，模型访问也可能受限")
                print("-"*50)
        
        # 测试3: 测试Azure OpenAI作为备用
        print("\n测试3: 测试Azure OpenAI (备用方案)")
        print("-"*50)
        try:
            azure_client = AzureOpenAI(
                api_key=config.AZURE_OPENAI_API_KEY,
                azure_endpoint=config.AZURE_OPENAI_API_BASE,
                api_version=config.AZURE_OPENAI_API_VERSION
            )
            
            print("尝试使用Azure OpenAI生成回复...")
            azure_completion = azure_client.chat.completions.create(
                model=config.AZURE_OPENAI_MODEL_NAME,
                messages=[{"role": "user", "content": "简短介绍一下你自己"}]
            )
            
            azure_content = azure_completion.choices[0].message.content
            print(f"Azure OpenAI备用响应成功: {azure_content[:100]}...")
            print("Azure OpenAI可以作为可靠的备用服务")
            print("-"*50)
        except Exception as e:
            print(f"Azure OpenAI备用也失败: {str(e)}")
            print("警告: 主备方案均失败，请检查配置")
            print("-"*50)
        
        # 测试4: 测试回退逻辑
        print("\n测试4: 测试备用回退逻辑")
        print("-"*50)
        
        # 如果OpenAI不可用，测试从服务层导入
        try:
            from service.llm_service import LLMService
            
            async def test_service():
                llm_service = LLMService()
                
                print("初始化OpenAI GPT-4.1...")
                try:
                    llm = llm_service.init_llm("openai-gpt41")
                    response = await llm.generate("简短介绍一下你自己")
                    print(f"LLM服务响应: {response[:100]}...")
                    print("如果看到此消息，回退机制运行正常!")
                except Exception as e:
                    print(f"LLM服务错误: {str(e)}")
                
            # 运行异步测试
            print("测试服务层回退逻辑...")
            asyncio.run(test_service())
        except Exception as e:
            print(f"服务层测试失败: {str(e)}")
        
        print("\n="*50)
        print("测试总结:")
        if not openai_available:
            print("- ❌ OpenAI API不可用")
            print("- ✅ 备用机制应该已激活")
        else:
            print("- ✅ OpenAI API可用")
            print("- ℹ️ 备用机制待命中")
        print("="*50)
        
    except Exception as e:
        import traceback
        print(f"测试过程中发生错误: {str(e)}")
        print(f"详细错误: {traceback.format_exc()}")

if __name__ == "__main__":
    test_openai_direct() 