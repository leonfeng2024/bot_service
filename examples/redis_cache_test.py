"""
Redis缓存测试 - 检索结果和final_check的缓存
"""
import sys
import os
import asyncio
import uuid

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.redis_tools import RedisTools
from Retriever.neo4j_retriever import Neo4jRetriever
from Retriever.opensearch_retriever import OpenSearchRetriever
from service.chat_service import ChatService

async def test_retriever_caching():
    """测试Neo4j和OpenSearch检索器的结果缓存"""
    # 生成一个测试用的UUID
    test_uuid = str(uuid.uuid4())
    print(f"测试UUID: {test_uuid}")
    
    # 初始化Redis工具
    redis = RedisTools()
    
    # 1. 确认Redis中没有此UUID的数据
    initial_data = redis.get(test_uuid)
    print(f"初始Redis数据: {initial_data}")
    
    # 2. 使用Neo4j检索器
    print("\n===== 测试Neo4j检索器缓存 =====")
    neo4j_retriever = Neo4jRetriever()
    query = "我想修改employee表的salary字段"
    print(f"执行查询: '{query}'")
    
    # 执行检索
    neo4j_results = await neo4j_retriever.retrieve(query, test_uuid)
    print(f"Neo4j检索到 {len(neo4j_results)} 条结果")
    
    # 查看Redis中的数据
    after_neo4j = redis.get(test_uuid)
    print(f"Neo4j检索后Redis数据: {after_neo4j}")
    
    # 3. 使用OpenSearch检索器
    print("\n===== 测试OpenSearch检索器缓存 =====")
    opensearch_retriever = OpenSearchRetriever()
    print(f"执行相同查询: '{query}'")
    
    # 执行检索
    opensearch_results = await opensearch_retriever.retrieve(query, test_uuid)
    print(f"OpenSearch检索到 {len(opensearch_results)} 条结果")
    
    # 查看Redis中的数据
    after_opensearch = redis.get(test_uuid)
    print(f"OpenSearch检索后Redis数据: {after_opensearch}")
    
    # 4. 测试ChatService的final_check
    print("\n===== 测试ChatService final_check缓存 =====")
    chat_service = ChatService()
    
    # 执行聊天
    chat_response = await chat_service.handle_chat("testuser", query, test_uuid)
    print(f"ChatService响应: {chat_response}")
    
    # 查看Redis中的数据
    final_data = redis.get(test_uuid)
    print(f"ChatService处理后Redis数据: {final_data}")
    
    # 5. 清理测试数据
    redis.delete(test_uuid)
    print("\n已清理测试数据")

if __name__ == "__main__":
    print("===== 开始测试Redis缓存功能 =====")
    asyncio.run(test_retriever_caching())
    print("===== 测试完成 =====")

"""
Redis缓存示例:

1. Neo4j检索后的Redis缓存:
{
    "neo4j": "表 employees 通过字段 employee_id 关联到表 employees_history 的字段 employee_id\n表 employees 通过字段 salary 关联到表 employees_history 的字段 old_salary"
}

2. OpenSearch检索后的Redis缓存:
{
    "neo4j": "表 employees 通过字段 employee_id 关联到表 employees_history 的字段 employee_id\n表 employees 通过字段 salary 关联到表 employees_history 的字段 old_salary",
    "opensearch": "Procedure 'UpdateEmployeeSalary':\nCREATE OR REPLACE FUNCTION UpdateEmployeeSalary(...) ..."
}

3. ChatService处理后的Redis缓存:
{
    "neo4j": "表 employees 通过字段 employee_id 关联到表 employees_history 的字段 employee_id\n表 employees 通过字段 salary 关联到表 employees_history 的字段 old_salary",
    "opensearch": "Procedure 'UpdateEmployeeSalary':\nCREATE OR REPLACE FUNCTION UpdateEmployeeSalary(...) ...",
    "final_check": "yes"
}
""" 