import asyncio
import os
import sys
import time
from service.llm_service import LLMService, GlobalLLMConfig
import config

async def test_global_llm_config():
    """测试全局LLM配置和自动切换功能"""
    print("\n" + "="*50)
    print("测试全局LLM配置管理")
    print("="*50)
    
    # 测试1: 检查初始全局配置
    print("\n测试1: 检查初始全局配置")
    print("-"*50)
    # 重置全局配置，确保测试从一致的状态开始
    GlobalLLMConfig.reset()
    initial_type = GlobalLLMConfig.get_current_llm_type()
    print(f"初始LLM类型: {initial_type}")
    
    # 测试2: 模拟OpenAI不可用场景
    print("\n测试2: 模拟OpenAI不可用场景")
    print("-"*50)
    
    # 先检查当前状态
    llm_service = LLMService()
    print(f"当前LLM类型: {llm_service.get_current_llm_type()}")
    
    if initial_type == "openai-gpt41":
        # 直接强制切换到azure-gpt4, 模拟OpenAI不可用
        print("手动设置全局配置为azure-gpt4 (模拟OpenAI不可用)")
        GlobalLLMConfig.set_current_llm_type("azure-gpt4")
        
        # 验证全局配置已更新
        new_type = GlobalLLMConfig.get_current_llm_type()
        print(f"更新后的LLM类型: {new_type}")
        
        # 验证LLMService是否感知到变化
        print(f"LLMService获取的当前类型: {llm_service.get_current_llm_type()}")
    else:
        print(f"注意: 系统已经默认使用Azure (原因: OpenAI可能已不可用)")
    
    # 测试3: 测试LLM服务自动感知配置变更
    print("\n测试3: 测试LLM服务自动感知配置变更")
    print("-"*50)
    
    # 创建一个新的LLM实例并生成内容
    try:
        llm = llm_service.get_llm()  # 应该自动使用当前全局配置
        current_type = llm_service.get_current_llm_type()
        print(f"使用的LLM类型: {current_type}")
        
        # 生成内容
        response = await llm.generate("简短介绍一下你自己")
        print(f"LLM响应: {response[:100]}...")
        
        # 切换回openai-gpt41并尝试生成
        if current_type != "openai-gpt41":
            try:
                print("\n尝试强制切换回OpenAI")
                llm_service.force_reset_llm("openai-gpt41")
                print(f"切换后的LLM类型: {llm_service.get_current_llm_type()}")
                
                response = await llm_service.get_llm().generate("另一个简短介绍")
                print(f"OpenAI响应: {response[:100]}...")
                print("OpenAI可用!")
            except Exception as e:
                print(f"切换回OpenAI失败，将继续使用Azure: {str(e)}")
    except Exception as e:
        print(f"LLM生成错误: {str(e)}")
    
    # 测试4: 测试自动错误处理和回退
    print("\n测试4: 测试自动错误处理和回退")
    print("-"*50)
    
    # 重置全局配置
    GlobalLLMConfig.reset()
    print(f"重置后的LLM类型: {GlobalLLMConfig.get_current_llm_type()}")
    
    # 强制初始化一个新的服务实例
    new_service = LLMService()
    new_service.llm_instance = None
    new_service.last_init_type = None
    
    # 尝试使用OpenAI生成内容，如果失败会自动切换到Azure
    try:
        llm = new_service.init_llm("openai-gpt41")
        response = await llm.generate("最后一次测试")
        print(f"最终使用的LLM类型: {new_service.get_current_llm_type()}")
        print(f"最终响应: {response[:100]}...")
    except Exception as e:
        print(f"最终测试失败: {str(e)}")
        
    print("\n" + "="*50)
    print("测试总结:")
    print(f"- 默认LLM类型: {GlobalLLMConfig.DEFAULT_LLM_TYPE}")
    print(f"- 当前活跃LLM类型: {GlobalLLMConfig.get_current_llm_type()}")
    if GlobalLLMConfig.get_current_llm_type() != GlobalLLMConfig.DEFAULT_LLM_TYPE:
        print("- 系统已自动处理了模型不可用的情况并切换到备用模型")
    print("="*50)

if __name__ == "__main__":
    asyncio.run(test_global_llm_config()) 