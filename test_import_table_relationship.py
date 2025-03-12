#!/usr/bin/env python3
import asyncio
import sys
import os
import json
from datetime import datetime

# 添加项目根目录到 Python 路径，确保能够导入 service 模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from service.neo4j_service import Neo4jService

# 添加一个辅助函数来处理Neo4j结果的JSON序列化
def make_serializable(obj):
    """将Neo4j结果对象转换为可JSON序列化的字典"""
    if isinstance(obj, dict):
        return {key: make_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [make_serializable(item) for item in obj]
    elif hasattr(obj, '__dict__'):
        # 对于具有__dict__属性的对象，尝试转换为字典
        try:
            return {key: make_serializable(value) for key, value in obj.__dict__.items() 
                    if not key.startswith('_')}
        except:
            return str(obj)
    else:
        # 对于无法序列化的对象，转换为字符串
        try:
            json.dumps(obj)
            return obj
        except:
            return str(obj)

async def test_get_v_relationships():
    """
    测试 get_v_relationships 方法，查询以 v_ 开头的节点及其关系
    """
    print(f"[{datetime.now()}] 开始测试 get_v_relationships 方法...")
    
    # 创建 Neo4jService 实例
    neo4j_service = Neo4jService()
    
    try:
        # 测试不同深度和限制的组合
        test_cases = [
            {"depth": 1, "limit": 10, "description": "深度1，限制10条结果"},
            {"depth": 2, "limit": 20, "description": "深度2，限制20条结果"},
            {"depth": 3, "limit": 5, "description": "深度3，限制5条结果"},
        ]
        
        for idx, case in enumerate(test_cases):
            print(f"\n[{datetime.now()}] 测试用例 #{idx+1}: {case['description']}")
            
            # 调用方法获取视图关系
            result = await neo4j_service.get_v_relationships(
                depth=case["depth"],
                limit=case["limit"]
            )
            
            # 验证结果
            node_count = len(result["nodes"])
            rel_count = len(result["relationships"])
            print(f"[{datetime.now()}] 查询结果: 找到 {node_count} 个节点和 {rel_count} 个关系")
            
            # 检查是否有结果
            if node_count == 0 and rel_count == 0:
                print("⚠️ 警告: 查询结果为空，请检查数据库中是否存在以 v_ 开头的节点")
                print("请确保已经运行 import_table_relationships 导入了关系数据")
            
            # 显示节点信息（最多显示3个）
            print("\n节点信息示例 (最多3个):")
            for i, node in enumerate(result["nodes"][:3]):
                node_name = node.get("name", "未知名称")
                node_labels = ", ".join(node.get("labels", []))
                print(f"  {i+1}. {node_name} [{node_labels}] (ID: {node.get('id', 'N/A')})")
            
            # 显示关系信息（最多显示3个）
            print("\n关系信息示例 (最多3个):")
            for i, rel in enumerate(result["relationships"][:3]):
                rel_type = rel.get("type", "未知类型")
                rel_props = rel.get("properties", {})
                description = rel_props.get("relationship_description", "无描述")
                created_at = rel_props.get("created_at", "未知时间")
                
                start_node_id = rel.get("start_node", "未知")
                end_node_id = rel.get("end_node", "未知")
                
                # 尝试查找节点名称
                start_node_name = "未知节点"
                end_node_name = "未知节点"
                for node in result["nodes"]:
                    if node.get("id") == start_node_id:
                        start_node_name = node.get("name", "未知名称")
                    if node.get("id") == end_node_id:
                        end_node_name = node.get("name", "未知名称")
                
                print(f"  {i+1}. {start_node_name} -[{rel_type}]-> {end_node_name}")
                print(f"     描述: {description}")
                print(f"     创建时间: {created_at}")
        
        # 保存结果到JSON文件
        if node_count > 0 or rel_count > 0:
            # 将结果转换为可序列化的数据
            serializable_result = make_serializable(result)
            
            # 保存为JSON文件
            first_result = test_cases[0]
            result_file = f"v_relationships_depth{first_result['depth']}_limit{first_result['limit']}.json"
            with open(result_file, "w", encoding="utf-8") as f:
                json.dump(serializable_result, f, indent=2, ensure_ascii=False)
            print(f"\n[{datetime.now()}] 已将查询结果保存到文件: {result_file}")
        else:
            print("\n[{datetime.now()}] 由于结果为空，未保存JSON文件")
        
    except Exception as e:
        print(f"[{datetime.now()}] ❌ 测试过程中发生错误: {str(e)}")
        import traceback
        print(traceback.format_exc())
    finally:
        # 关闭连接
        neo4j_service.close()
        print(f"\n[{datetime.now()}] 测试完成，Neo4j 连接已关闭")

# 用于测试数据导入函数
async def test_import_data_first():
    """先导入一些数据，确保有测试数据可用"""
    print(f"[{datetime.now()}] 开始导入测试数据...")
    
    # 创建 Neo4jService 实例
    neo4j_service = Neo4jService()
    
    try:
        # 准备一些视图关系数据
        relationships_json = {
            "v_ENC_INS_SOS.ho_ins_type": "v_HO_INS_TYPE.seq_no",
            "v_ENC_INS_SOS.INS_NO": "v_AYU_COM_INS_MST_EXP_ALL_SAISHIN.INS_NO",
            "v_ENC_INS_SOS.INS_NO": "v_UINS_INS_SOS_4SF.INS_NO",
            "v_AYU_COM_INS_MST_EXP_ALL_SAISHIN.reln_ins_no": "v_AYU_COM_INS_MST_EXP_HONIN_SAISHIN.reln_ins_no",
            "v_UINS_INS_SOS_4SF.DUMMY_SOS_CD": "v_PBI_4SF_SQLSERVER.DUMMY_SOS_CD",
            "v_UINS_INS_SOS_4SF.DUMMY_SOS_CD": "v_AYU_M_SOS_MST_FIX_JPBUDIST.sos_cd"
        }
        
        # 导入关系
        success = await neo4j_service.import_table_relationships(relationships_json)
        
        if success:
            print(f"[{datetime.now()}] ✅ 成功导入测试数据")
        else:
            print(f"[{datetime.now()}] ❌ 导入测试数据失败")
    
    except Exception as e:
        print(f"[{datetime.now()}] ❌ 导入数据过程中发生错误: {str(e)}")
        import traceback
        print(traceback.format_exc())
    finally:
        # 关闭连接
        neo4j_service.close()

# 运行两个测试函数
async def main():
    # 先导入数据，再测试查询
    await test_import_data_first()
    await test_get_v_relationships()

if __name__ == "__main__":
    # 运行主函数
    asyncio.run(main())
