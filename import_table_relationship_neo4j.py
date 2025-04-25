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

# 示例表关系数据 - 这些关系将被导入到Neo4j数据库中
SAMPLE_RELATIONSHIPS = {
    # 员工和部门关系
    "employees.employee_id": "departments.department_id",
    "employees.manager_id": "employees.employee_id",
    "departments.manager_id": "employees.employee_id",
    
    # 职位历史
    "job_history.employee_id": "employees.employee_id",
    "job_history.department_id": "departments.department_id",
    "job_history.job_id": "jobs.job_id",
    
    # 地理位置
    "countries.region_id": "regions.region_id",
    "locations.country_id": "countries.country_id",
    "departments.location_id": "locations.location_id",
    
    # 视图关系
    "v_employee_details.employee_id": "employees.employee_id", 
    "v_employee_details.department_id": "departments.department_id", 
    "v_department_summary.department_id": "departments.department_id",
    "v_salary_report.job_id": "jobs.job_id",
    
    # 存储过程关系
    "p_update_employee.employee_id": "employees.employee_id",
    "p_transfer_employee.employee_id": "employees.employee_id",
    "p_transfer_employee.department_id": "departments.department_id",
    "p_calculate_bonus.employee_id": "employees.employee_id",
    "p_get_department_staff.department_id": "departments.department_id"
}

async def import_table_relationships():
    """
    导入表关系到Neo4j数据库
    """
    print(f"[{datetime.now()}] 开始导入表关系到Neo4j数据库...")
    
    # 初始化Neo4j服务
    neo4j_service = Neo4jService()
    
    try:
        # 1. 检查Neo4j连接
        if not neo4j_service.neo4j.connected:
            print("尝试重新连接Neo4j...")
            neo4j_service.neo4j._connect()
            if not neo4j_service.neo4j.connected:
                print("无法连接到Neo4j数据库，请检查配置")
                return False
        
        # 2. 清理现有数据 - 谨慎操作，这会删除所有表和关系
        print("清理现有数据...")
        neo4j_service.neo4j.execute_query("MATCH (n) DETACH DELETE n")
        
        # 3. 导入表和视图节点
        print("导入表和视图节点...")
        
        # 收集所有的表和视图名称
        all_tables = set()
        for relation in SAMPLE_RELATIONSHIPS.items():
            source, target = relation
            source_table = source.split('.')[0]
            target_table = target.split('.')[0]
            all_tables.add(source_table)
            all_tables.add(target_table)
        
        # 创建所有表和视图节点
        for table in all_tables:
            # 确定节点标签 - 视图以v_开头
            node_label = "View" if table.startswith('v_') else "Table"
            
            # 创建节点
            query = f"""
            MERGE (t:{node_label} {{name: $table_name}})
            RETURN t
            """
            neo4j_service.neo4j.execute_query(query, parameters={"table_name": table})
            print(f"创建{node_label}节点: {table}")
        
        # 4. 导入关系
        print(f"导入 {len(SAMPLE_RELATIONSHIPS)} 个表关系...")
        
        success = await neo4j_service.import_table_relationships(SAMPLE_RELATIONSHIPS)
        
        if success:
            print("表关系导入成功")
            
            # 5. 验证导入的数据
            print("验证导入的数据...")
            node_count = neo4j_service.neo4j.get_node_count()
            relationship_count = neo4j_service.neo4j.get_relationship_count()
            
            print(f"节点数量: {node_count}")
            print(f"关系数量: {relationship_count}")
            
            # 6. 查询一些示例关系
            print("查询示例关系...")
            query = """
            MATCH (a)-[r:RELATED_TO]->(b)
            RETURN a.name as source_table, b.name as target_table, 
                   r.source_field as source_field, r.target_field as target_field
            LIMIT 10
            """
            
            results = neo4j_service.neo4j.execute_query(query)
            for result in results:
                print(f"{result.get('source_table')}.{result.get('source_field')} -> {result.get('target_table')}.{result.get('target_field')}")
                
            return True
        else:
            print("表关系导入失败")
            return False
            
    except Exception as e:
        import traceback
        print(f"导入表关系时出错: {str(e)}")
        print(traceback.format_exc())
        return False
    finally:
        try:
            # 关闭Neo4j连接
            neo4j_service.close()
        except:
            pass

def get_visualization_query():
    """获取用于在Neo4j浏览器中可视化图的Cypher查询"""
    return """
    MATCH (a:Table)-[r:RELATED_TO]->(b:Table)
    RETURN a, r, b
    LIMIT 100
    """

async def main():
    # 1. 清除所有数据
    await clear_all_data()
    
    # 2. 导入新的关系数据
    await import_table_relationships()
    
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
