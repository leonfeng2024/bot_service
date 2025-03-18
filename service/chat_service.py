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
请根据用户输入内容分析设备类型或处理方式，使用JSON格式返回分类结果。按以下步骤处理：
 
1. 设备异常处理判断（优先级从高到低）：
- 风电关键词：["风机", "叶片", "塔筒", "变桨系统", "齿轮箱", "偏航系统"]
- 水电关键词：["水轮机", "导水机构", "压力钢管", "调压室", "尾水管", "闸门"]
- 煤电关键词：["锅炉", "汽轮机", "磨煤机", "脱硫塔", "电除尘", "输煤系统"]
 
2. 起票处理判断（优先级高于设备异常处理）：
- 工单关键词：["工单", "ticket", "起票", "提交问题", "报修单", "故障单"]
- 系统关键词：["第三方系统", "帐票系统", "ERP系统", "OA系统"]
- 行为关键词：["发起", "创建", "提交", "申请", "登记"]
 
3. 行业术语识别：
- 风电特征词：["MW级机组", "扫频故障", "变流器", "风功率预测"]
- 水电特征词：["水头损失", "空蚀现象", "调峰运行", "库容曲线"]
- 煤电特征词：["SOFC排放", "飞灰含碳量", "SCR脱硝", "汽水系统"]
- 起票特征词：["工单号", "审批流程", "紧急程度", "优先级"]
 
4. 上下文分析：
- 时间特征：包含"风速突变"、"冰冻天气"等环境因素倾向风电 
- 参数特征：涉及"水头高度"、"流量调节"等参数倾向水电 
- 化学特征：出现"煤粉浓度"、"硫化物"等指标倾向煤电 
- 流程特征：出现"审批人"、"处理人"、"截止时间"等倾向起票处理 
 
5. 异常处理模式匹配：
- 风电典型故障："齿轮箱过热>85℃","发电机绝缘失效<100MΩ"
- 水电典型故障："导轴承摆度>0.5mm","密封环漏水>30L/min" 
- 煤电典型故障："炉管壁温>450℃","烟气SO2>200mg/Nm³"
- 起票典型特征："工单状态：待处理","紧急程度：高"
 
处理规则：
1. 同时匹配多个类别时，按起票处理>风电>水电>煤电的优先级处理 
2. 出现否定词（"不涉及"/"非"）时跳过该类别 
3. 包含维修阶段术语（"定期维护"/"预防性检修"）不触发异常判定 
4. 起票处理需明确包含工单创建或提交行为，避免误判 
 
请严格按照以下JSON格式返回结果，不要包含任何其他文字：
{"category":"wind", "message":"OK"}  # 匹配风电异常 
{"category":"water", "message":"OK"}  # 匹配水电异常 
{"category":"fire", "message":"OK"}   # 匹配煤电异常 
{"category":"ticket", "message":"OK"} # 匹配起票处理 
{"category":"unknown", "message":"NG"} # 无匹配 
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
            if intent_result["category"] == "wind":
                return {
                    "status": "success",
                    "username": self.bot_name,
                    "message": "已经连接到风力发电知识库"
                }
            elif intent_result["category"] == "water":
                return {
                    "status": "success",
                    "username": self.bot_name,
                    "message": "已经连接到水力发电知识库"
                }
            elif intent_result["category"] == "fire":
                return {
                    "status": "success",
                    "username": self.bot_name,
                    "message": "已经连接到火力发电知识库"
                }
            elif intent_result["category"] == "ticket":
                return {
                    "status": "success",
                    "username": self.bot_name,
                    "message": "启动起票agent"
                }
            elif intent_result["category"] == "unknown":
                return {
                    "status": "success",
                    "username": self.bot_name,
                    "message": "我可以提供 风力|水力|火力 的知识问答，如果以上3中类型不是你想要提的问题，你还可以跟我说起票处理"
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