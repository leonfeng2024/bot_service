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

async def clear_all_data():
    """清除Neo4j数据库中的所有节点和关系"""
    print(f"[{datetime.now()}] 开始清除数据库中的所有数据...")
    
    neo4j_service = Neo4jService()
    try:
        # 执行清除所有数据的Cypher查询
        query = "MATCH (n) DETACH DELETE n"
        neo4j_service.neo4j.execute_query(query)
        print(f"[{datetime.now()}] ✅ 成功清除所有数据")
    except Exception as e:
        print(f"[{datetime.now()}] ❌ 清除数据时发生错误: {str(e)}")
        import traceback
        print(traceback.format_exc())
    finally:
        neo4j_service.close()

async def import_relationships(relationships_json):
    """导入关系数据"""
    print(f"[{datetime.now()}] 开始导入关系数据...")
    
    neo4j_service = Neo4jService()
    try:
        # 导入关系
        success = await neo4j_service.import_table_relationships(relationships_json)
        
        if success:
            print(f"[{datetime.now()}] ✅ 成功导入关系数据")
        else:
            print(f"[{datetime.now()}] ❌ 导入关系数据失败")
    except Exception as e:
        print(f"[{datetime.now()}] ❌ 导入数据过程中发生错误: {str(e)}")
        import traceback
        print(traceback.format_exc())
    finally:
        neo4j_service.close()

def get_visualization_query():
    """获取用于在Neo4j浏览器中可视化图的Cypher查询"""
    return """
    MATCH (a:Table)-[r:RELATED_TO]->(b:Table)
    RETURN a, r, b
    LIMIT 100
    """

async def main():
    # 定义关系数据
    relationships_json = {
        "employee_details.employee_id": "employees.employee_id", 
        "employee_details.department_name": "departments.department_name", 
        "high_salary_employees.employee_id": "employees.employee_id", 
        "active_accounts.account_id": "accounts.account_id", 
        "transaction_summary.account_id": "accounts.account_id", 
        "transaction_summary.account_name": "accounts.account_name", 
        "employee_history_log.employee_id": "employees.employee_id", 
        "employee_history_log.changed_by": "employees.employee_id", 
        "GetEmployeeDetails.employee_id": "employee_details.employee_id", 
        "GetAccountTransactions.account_id": "transactions.account_id", 
        "GetAccountTransactions.account_name": "accounts.account_name", 
        "UpdateEmployeeSalary.employee_id": "employees.employee_id", 
        "UpdateEmployeeSalary.changed_by": "employees.employee_id" 
    }
    
    # 1. 清除所有数据
    await clear_all_data()
    
    # 2. 导入新的关系数据
    await import_relationships(relationships_json)
    
    # 3. 输出可视化查询
    print("\n=== Neo4j浏览器可视化查询 ===")
    print("在Neo4j浏览器中执行以下查询以查看关系图：")
    print(get_visualization_query())
    print("\n提示：")
    print("1. 在Neo4j浏览器中执行上述查询")
    print("2. 点击结果面板中的'Graph'视图")
    print("3. 可以拖动节点调整布局")
    print("4. 可以点击节点展开/收起详细信息")

if __name__ == "__main__":
    # 运行主函数
    asyncio.run(main())
