import re
import json
from collections import defaultdict
from sqlalchemy import create_engine, inspect, text


ddl_file = r"C:\Users\pinjing.wu\Desktop\ddl_test2.sql"


def remove_sql_comments(any_sql: str) -> str:
    """コメントを削除する"""
    __pattern = re.compile(r"--.*?$|/\*.*?\*/", re.MULTILINE | re.DOTALL)

    return re.sub(pattern=__pattern, repl="", string=any_sql).strip()

def clean_sql(any_sql):
    """SQL文をクリーンアップし、不要なスペースや改行を削除する"""
    any_sql = re.sub(pattern=r"\s+", repl=" ", string=any_sql)  # 多个空格变成一个
    any_sql = any_sql.replace("[", "").replace("]", "") # 去掉中括号
    any_sql = any_sql.replace(" ,", ",")  # 处理逗号前的多余空格
    return any_sql.strip()

def extract_tables_from_view(view_sql):
    """ビューのSQLを解析し、テーブル名を抽出する（FROMおよびJOINに対応）"""
    table_pattern = re.compile(r"(?:FROM|JOIN)\s+(?:\(\s*)?(?!SELECT\b)(\w+(?:\.\w+)?)", flags=re.IGNORECASE)
    tables = set()
    for match in table_pattern.finditer(view_sql):
        tables.add((match.group(1) or match.group(2)).split(".")[-1])
    return list(tables)

def parse_relation_from_ddl(ddl):
    """DDLファイルを解析し、テーブルとフィールドの関係、およびビューとテーブルの関係を抽出する"""

    # SQLを前処理し、単一行に結合する。
    ddl = clean_sql(remove_sql_comments(ddl))

    table_fields = defaultdict(list)
    view_tables = defaultdict(list)

    # "CREATE TABLE"を解析
    table_pattern = re.compile(pattern=r"CREATE TABLE (\w+(?:\.\w+)?)\s*\((.*?)\)\s*[\w\s]+(;|GO)", flags=re.IGNORECASE)
    for match in table_pattern.finditer(ddl):
        table_name = match.group(1).split(".")[-1]
        fields_block = match.group(2)

        # フィールド名の抽出
        fields = []
        field_lines = re.split(r",\s*(?![^(]*\))", fields_block)
        for line in field_lines:
            line = line.strip()
            if not line or "PRIMARY KEY" in line.upper():  # 跳过主键
                continue
            field_name = re.split(r"\s+", line, maxsplit=1)[0]
            fields.append(field_name)

        table_fields[table_name] = fields
        print("fields len: ", str(len(fields)))

    # "CREATE VIEW"を解析
    view_pattern = re.compile(pattern=r"CREATE (OR REPLACE )?VIEW (\w+(?:\.\w+)?) AS (.*?)(;|GO)$", flags=re.IGNORECASE)
    for match in view_pattern.finditer(ddl):
        view_name = match.group(2).split(".")[-1]
        view_sql = match.group(3)
        tables = extract_tables_from_view(view_sql)
        view_tables[view_name] = tables

    return table_fields, view_tables


with open(ddl_file, "r", encoding="utf-8") as f:
    ddl = f.read()

table_fields, view_tables = parse_relation_from_ddl(ddl)

# print result
print("Table-Field关系:")
print(json.dumps(table_fields, indent=4, ensure_ascii=False))

print("View-Table关系:")
print(json.dumps(view_tables, indent=4, ensure_ascii=False))

# 判断Langchain用的表和view是否存在，不存在则创建
db_uri = "postgresql+psycopg2://postgres:123456@localhost:5432/verify5"
engine = create_engine(db_uri)
inspector = inspect(engine)
all_tables = inspector.get_table_names()
all_views = inspector.get_view_names()

not_in_tables = [item for item in ["table_fields", "view_tables", "dataset_view_tables"] if item not in all_tables]
not_in_views = [item for item in ["v_view_table_field", "v_dataset_view_table_field"] if item not in all_views]

create_sql = []
if len(not_in_tables) > 0:
    if "table_fields" in not_in_tables:
        create_sql.append("""
            CREATE TABLE table_fields (
                physical_name TEXT NOT NULL, 
                logical_name TEXT, 
                field TEXT NOT NULL, 
                field_jpn TEXT
            );""")

    if "view_tables" in not_in_tables:
        create_sql.append("""
            CREATE TABLE view_tables (
                physical_name TEXT NOT NULL,
                logical_name TEXT,
                table_nm TEXT NOT NULL,
                table_nm_jpn TEXT
            );""")

    if "dataset_view_tables" in not_in_tables:
        create_sql.append("""
            CREATE TABLE dataset_view_tables (
                physical_name TEXT NOT NULL,
                logical_name TEXT,
                nm TEXT NOT NULL,
                nm_jpn TEXT
            );""")

if len(not_in_views) > 0:
    if "v_view_table_field" in not_in_views:
        create_sql.append("""
            create view v_view_table_field as
            select vt.physical_name as view_physical_name, vt.logical_name as view_logical_name, vt.table_nm as table_physical_name, vt.table_nm_jpn as table_logical_name, tf.field as field, tf.field_jpn as field_jpn
            from view_tables as vt 
            left join table_fields as tf on vt.table_nm=tf.physical_name
            order by vt.physical_name, vt.table_nm, tf.field;
        """)

    if "v_dataset_view_table_field" in not_in_views:
        create_sql.append("""
            create view v_dataset_view_table_field as
            select dvf.physical_name as ds_physical_name, dvf.logical_name as ds_logical_name, dvf.nm as view_name, alv.table_physical_name as table_name, alv.field as field, alv.field_jpn as field_jpn 
            from dataset_view_tables as dvf
            left join 
            (select * from v_view_table_field
            union all 
            select tf.physical_name as view_physical_name, tf.logical_name as view_logical_name, tf.physical_name as table_physical_name, tf.logical_name as table_logical_name, tf.field as field, tf.field_jpn as field_jpn 
            from table_fields as tf) as alv on dvf.nm=alv.view_physical_name
            order by dvf.physical_name, dvf.nm, alv.field;
        """)

if len(create_sql) > 0:
    with engine.connect() as conn:
        for s in create_sql:
            conn.execute(text(s))
        conn.commit()

# input data
if len(table_fields) > 0:
    for table_name, field_list in table_fields.items():
        with engine.connect() as conn:
            # delete the old data
            delete_sql = "DELETE FROM table_fields WHERE physical_name = :physical_name"
            conn.execute(statement=text(delete_sql), parameters={"physical_name": table_name})
            # insert new data
            insert_sql = "INSERT INTO table_fields (physical_name, field) VALUES (:physical_name, :field)"
            for f in field_list:
                conn.execute(statement=text(insert_sql), parameters={"physical_name": table_name, "field": f})

            conn.commit()


if len(view_tables) > 0:
    for view_name, table_list in view_tables.items():
        with engine.connect() as conn:
            # delete the old data
            delete_sql = "DELETE FROM view_tables WHERE physical_name = :physical_name"
            conn.execute(statement=text(delete_sql), parameters={"physical_name": view_name})
            # insert new data
            insert_sql = "INSERT INTO view_tables (physical_name, table_nm) VALUES (:physical_name, :table_nm)"
            for t in table_list:
                conn.execute(statement=text(insert_sql), parameters={"physical_name": view_name, "table_nm": t})

            conn.commit()

