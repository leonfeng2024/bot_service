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
        分析用户意图
        """
        intent_prompt = """
任务描述
根据用户输入内容，判断是否为数据库表结构变更影响咨询，重点关注表字段变更相关描述。使用 JSON 格式返回分类结果。

判断逻辑与规则
1. 变更类型识别
优先级从高到低，识别以下关键词：

表字段变更关键词:
["column", "field", "添加字段", "删除字段", "修改字段", "重命名字段", "添加约束", "删除约束", "修改列类型", "调整索引"]
结构变更关键词:
["ALTER TABLE", "字段删除", "修改列类型", "添加约束", "重命名字段", "调整索引", "表结构变更", "表字段调整"]
2. 专业术语识别
识别以下数据库特征词，进一步确认变更意图：
["DDL操作", "Schema版本", "元数据变更", "ONLINE DDL", "触发器失效", "数据迁移", "表结构设计", "字段定义"]

3. 上下文分析
通过以下特征判断是否为变更影响咨询：

包含具体操作描述，如：
"修改字段长度", "删除非空约束", "添加唯一索引", "将varchar(50)改为varchar(100)"
包含影响询问，如：
"会影响现有数据吗", "是否需要停机", "是否兼容旧版本"
4. 典型模式匹配
匹配以下典型模式：

"将varchar(50)改为varchar(100)会影响现有数据吗"
"删除字段后，触发器是否会失效"
"添加唯一约束是否会导致数据重复报错"
5. 排除规则
以下情况返回 unknown：

出现否定词，如："暂未执行", "仅测试", "未涉及变更"
包含理论讨论术语，如："设计模式", "范式理论", "数据库优化"
未明确包含变更操作和影响询问两个要素
返回规则
严格按照以下 JSON 格式返回结果：

表结构变更影响咨询:
{"category":"schema_change", "message":"OK"}
非变更影响咨询:
{"category":"unknown", "message":"NG"}
示例
示例 1
用户输入:
"我想将表字段从varchar(50)改为varchar(100)，这会影响现有数据吗？"
返回结果:
{"category":"schema_change", "message":"OK"}

示例 2
用户输入:
"数据库设计模式中，第三范式如何应用？"
返回结果:
{"category":"unknown", "message":"NG"}

示例 3
用户输入:
"暂未执行表字段变更，仅测试了一下。"
返回结果:
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
                    "message": "我可以提供数据库表变更依赖关系的查询服务，请告诉你需要变更那个表？"
                }
            
            # 获取相关文档
            docs = await self.rag_service.retrieve(query)
            
            # 构建prompt
            prompt = f"""请基于以下知识库内容回答用户的问题。请仅使用提供的知识库内容进行回答。

知识库内容：
{chr(10).join([f'- {doc}' for doc in docs])}

用户问题：{query}

请按照以下格式回答：

1. 相关表关系概述：
   [请简要概述涉及的表之间的关系]

2. 详细关联说明：
   [请列出具体的表关联细节，每个关联一行]

3. 补充说明：
   [如果有任何额外的重要信息，请在这里说明]

要求：
- 保持回答简洁准确
- 避免重复内容
- 如果知识库中没有相关信息，请明确说明
- 使用markdown格式来增强可读性
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