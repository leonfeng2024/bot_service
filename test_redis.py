#!/usr/bin/env python3
"""
test_redis.py - 用于从Redis中获取、修改或删除指定UUID的缓存数据

用法:
    python test_redis.py get <uuid>               # 获取UUID的缓存数据
    python test_redis.py set <uuid> <key> <value> # 设置UUID中指定键的值
    python test_redis.py del <uuid>               # 删除UUID的所有缓存数据
    python test_redis.py del <uuid> <key>         # 删除UUID中指定键的值
    python test_redis.py demo                     # 创建一个示例数据用于测试

示例:
    python test_redis.py get abc123-4567-890a-abcdef123456
    python test_redis.py set abc123-4567-890a-abcdef123456 final_check yes
    python test_redis.py del abc123-4567-890a-abcdef123456
    python test_redis.py del abc123-4567-890a-abcdef123456 opensearch
"""

import sys
import json
import argparse
import uuid as uuid_lib
from tools.redis_tools import RedisTools
from config import JWT_EXPIRATION

def get_redis_data(uuid):
    """获取指定UUID的Redis缓存数据"""
    if not uuid:
        print("错误: 未提供UUID参数")
        return None
    
    # 初始化Redis工具
    redis = RedisTools()
    
    # 获取数据
    data = redis.get(uuid)
    return data

def set_redis_data(uuid, key, value):
    """设置指定UUID的Redis缓存数据中的键值"""
    if not uuid or not key:
        print("错误: 未提供必要的参数")
        return False
    
    # 初始化Redis工具
    redis = RedisTools()
    
    # 获取当前数据
    current_data = redis.get(uuid) or {}
    
    # 更新指定键的值
    # 尝试将value转换为适当的类型
    try:
        # 尝试解析为JSON
        parsed_value = json.loads(value)
        current_data[key] = parsed_value
    except (json.JSONDecodeError, TypeError):
        # 如果不是JSON，则使用原始字符串
        current_data[key] = value
    
    # 保存回Redis
    success = redis.set(uuid, current_data)
    return success

def delete_redis_data(uuid, key=None):
    """删除指定UUID的Redis缓存数据，或只删除指定的键"""
    if not uuid:
        print("错误: 未提供UUID参数")
        return False
    
    # 初始化Redis工具
    redis = RedisTools()
    
    if key:
        # 只删除特定键
        current_data = redis.get(uuid)
        if not current_data:
            print(f"未找到UUID: {uuid} 的缓存数据")
            return False
        
        if key not in current_data:
            print(f"UUID: {uuid} 的缓存中不存在键: {key}")
            return False
        
        # 删除指定键
        del current_data[key]
        # 保存回Redis
        return redis.set(uuid, current_data)
    else:
        # 删除整个UUID的数据
        return redis.delete(uuid)

def create_demo_data():
    """创建一个示例数据用于测试"""
    # 生成一个随机UUID
    demo_uuid = str(uuid_lib.uuid4())
    
    # 初始化Redis工具
    redis = RedisTools()
    
    # 创建示例数据
    demo_data = {
        "user_id": 123,
        "role": "user",
        "username": "testuser",
        "login_time": 1642435317,
        "neo4j": "表 employees 通过字段 employee_id 关联到表 employees_history 的字段 employee_id\n表 employees 通过字段 salary 关联到表 employees_history 的字段 old_salary",
        "opensearch": "Procedure 'UpdateEmployeeSalary':\nCREATE OR REPLACE FUNCTION UpdateEmployeeSalary(\n    emp_id INT,\n    new_salary DECIMAL(10, 2),\n    changed_by INT \n) RETURNS VOID AS $$\nDECLARE \n    old_salary DECIMAL(10, 2);\nBEGIN \n    -- 获取当前工资 \n    SELECT salary INTO old_salary FROM employees WHERE employee_id = emp_id;\n    \n    -- 更新员工工资 \n    UPDATE employees SET salary = new_salary WHERE employee_id = emp_id;\n    \n    -- 插入历史记录 \n    INSERT INTO employees_history (\n        employee_id,\n        old_salary,\n        new_salary,\n        changed_by \n    ) VALUES (\n        emp_id,\n        old_salary,\n        new_salary,\n        changed_by \n    );\nEND;\n$$ LANGUAGE plpgsql;\n相关表: ['employees', 'employees_history']",
        "final_check": "yes",
        "procedure": "CREATE PROCEDURE TEMP_P AS SELECT * FROM employees",
        "table_column": ["employee_id", "employee_name", "salary"]
    }
    
    # 保存示例数据
    success = redis.set(demo_uuid, demo_data, JWT_EXPIRATION)
    
    if success:
        print(f"成功创建示例数据，UUID: {demo_uuid}")
        print("数据将在 Redis 中保存 {} 秒 (与 JWT 令牌过期时间相同)".format(JWT_EXPIRATION))
        print("\n您可以使用以下命令查看数据:")
        print(f"python test_redis.py get {demo_uuid}")
        return demo_uuid
    else:
        print("创建示例数据失败")
        return None

def format_data(data):
    """格式化缓存数据以便友好输出"""
    if not data:
        return "未找到指定UUID的缓存数据"
    
    result = []
    
    # 格式化基本用户信息
    if "user_id" in data:
        result.append("用户信息:")
        result.append(f"  用户ID: {data.get('user_id')}")
        result.append(f"  角色: {data.get('role')}")
        result.append(f"  用户名: {data.get('username')}")
        result.append(f"  登录时间: {data.get('login_time')}")
    
    # Neo4j 检索结果
    if "neo4j" in data:
        result.append("\nNeo4j检索结果:")
        result.append(f"  {data['neo4j']}")
    
    # OpenSearch 检索结果
    if "opensearch" in data:
        result.append("\nOpenSearch检索结果:")
        # 限制输出长度，避免内容过长
        content = data['opensearch']
        if len(content) > 500:
            content = content[:500] + "... (内容已截断)"
        result.append(f"  {content}")
    
    # 最终检查结果
    if "final_check" in data:
        result.append("\n最终检查结果:")
        result.append(f"  {data['final_check']}")
    
    # 存储过程信息
    if "procedure" in data:
        result.append("\n存储过程信息:")
        procedure_content = data['procedure']
        if len(procedure_content) > 200:
            procedure_content = procedure_content[:200] + "... (内容已截断)"
        result.append(f"  {procedure_content}")
    
    # 表字段信息
    if "table_column" in data:
        result.append("\n表字段信息:")
        column_list = data['table_column']
        if isinstance(column_list, list):
            result.append(f"  {', '.join(column_list)}")
        else:
            result.append(f"  {column_list}")
    
    # 其他字段
    other_fields = [k for k in data.keys() if k not in ["user_id", "role", "username", "login_time", "neo4j", "opensearch", "final_check", "procedure", "table_column"]]
    if other_fields:
        result.append("\n其他数据:")
        for field in other_fields:
            value = data[field]
            # 如果值是复杂对象，以JSON格式显示
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False)
            result.append(f"  {field}: {value}")
    
    return "\n".join(result)

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Redis数据操作工具')
    
    # 添加子命令
    subparsers = parser.add_subparsers(dest='command', help='命令')
    
    # get命令
    get_parser = subparsers.add_parser('get', help='获取UUID的缓存数据')
    get_parser.add_argument('uuid', help='要查询的UUID')
    
    # set命令
    set_parser = subparsers.add_parser('set', help='设置UUID中指定键的值')
    set_parser.add_argument('uuid', help='要修改的UUID')
    set_parser.add_argument('key', help='要设置的键')
    set_parser.add_argument('value', help='要设置的值')
    
    # del命令
    del_parser = subparsers.add_parser('del', help='删除UUID的缓存数据')
    del_parser.add_argument('uuid', help='要删除的UUID')
    del_parser.add_argument('key', nargs='?', help='要删除的键(可选)')
    
    # list命令
    list_parser = subparsers.add_parser('list', help='列出所有UUID列表 (暂不支持)')
    
    # demo命令
    demo_parser = subparsers.add_parser('demo', help='创建示例数据用于测试')
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    
    return parser.parse_args()

def main():
    """主函数"""
    args = parse_args()
    
    if args.command == 'get':
        uuid = args.uuid
        print(f"正在查询UUID: {uuid} 的缓存数据...\n")
        
        # 获取数据
        data = get_redis_data(uuid)
        
        # 输出数据
        if data:
            print("已找到缓存数据:")
            print(format_data(data))
            
            # 打印原始JSON
            print("\n原始JSON数据:")
            print(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            print(f"未找到UUID: {uuid} 的缓存数据")
    
    elif args.command == 'set':
        uuid = args.uuid
        key = args.key
        value = args.value
        
        print(f"正在设置UUID: {uuid} 的键: {key} 的值为: {value}")
        success = set_redis_data(uuid, key, value)
        
        if success:
            print(f"成功设置UUID: {uuid} 的键: {key}")
            # 显示更新后的数据
            data = get_redis_data(uuid)
            print("\n更新后的数据:")
            print(format_data(data))
        else:
            print(f"设置UUID: {uuid} 的键: {key} 失败")
    
    elif args.command == 'del':
        uuid = args.uuid
        key = args.key
        
        if key:
            print(f"正在删除UUID: {uuid} 的键: {key}")
            success = delete_redis_data(uuid, key)
            if success:
                print(f"成功删除UUID: {uuid} 的键: {key}")
                # 显示更新后的数据
                data = get_redis_data(uuid)
                if data:
                    print("\n更新后的数据:")
                    print(format_data(data))
            else:
                print(f"删除UUID: {uuid} 的键: {key} 失败")
        else:
            print(f"正在删除UUID: {uuid} 的所有数据")
            success = delete_redis_data(uuid)
            if success:
                print(f"成功删除UUID: {uuid} 的所有数据")
            else:
                print(f"删除UUID: {uuid} 的所有数据失败")
    
    elif args.command == 'list':
        print("列出所有UUID功能尚未实现")
    
    elif args.command == 'demo':
        create_demo_data()

if __name__ == "__main__":
    main() 