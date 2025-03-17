import sys
import openpyxl
import re
import os
from sqlalchemy import create_engine, inspect, text


file_path = r"/temp/PD011_02納入_納入計画実績(MR向け).xlsx"
_file_name_list = os.path.basename(file_path).split(".")[0].split("_")
dataset_name = _file_name_list[0] + "_" + _file_name_list[2]

wb = openpyxl.load_workbook(file_path)
ws = wb["資材一覧"]
header_row = 2
header = [cell.value for cell in ws[header_row]]

if "テーブル名" in header:
    table_col_idx = header.index("テーブル名") + 1
else:
    sys.exit(0)

filtered = set()
for row in range(header_row + 1, ws.max_row + 1):
    table_cell = ws.cell(row=row, column=table_col_idx)

    if not (table_cell.font and table_cell.font.strike):
        if table_cell.value is not None:
            filtered.add(table_cell.value)

filtered_set = {s for s in filtered if re.search(r"[a-zA-Z]", s)}
print(filtered_set)

db_uri = "postgresql+psycopg2://postgres:123456@localhost:5432/verify5"
engine = create_engine(db_uri)
inspector = inspect(engine)
all_tables = inspector.get_table_names()
all_views = inspector.get_view_names()

not_in_tables = [item for item in ["dataset_view_tables"] if item not in all_tables]
not_in_views = [item for item in ["v_dataset_view_table_field"] if item not in all_views]

create_sql = []
if len(not_in_tables) > 0:
    if "dataset_view_tables" in not_in_tables:
        create_sql.append("""
            CREATE TABLE dataset_view_tables (
                physical_name TEXT NOT NULL,
                logical_name TEXT,
                nm TEXT NOT NULL,
                nm_jpn TEXT
            );""")

if len(not_in_views) > 0:
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
if len(filtered_set) > 0:
    with engine.connect() as conn:
        # delete the old data
        delete_sql = "DELETE FROM dataset_view_tables WHERE physical_name = :physical_name"
        conn.execute(statement=text(delete_sql), parameters={"physical_name": dataset_name})
        # insert new data
        insert_sql = "INSERT INTO dataset_view_tables (physical_name, nm) VALUES (:physical_name, :nm)"
        for t in filtered_set:
            conn.execute(statement=text(insert_sql), parameters={"physical_name": dataset_name, "nm": t})

        conn.commit()

