CREATE TABLE table_fields (
    physical_name TEXT NOT NULL,
    logical_name TEXT,
    field TEXT NOT NULL,
    field_jpn TEXT
);

CREATE TABLE view_tables (
    physical_name TEXT NOT NULL,
    logical_name TEXT,
    table_nm TEXT NOT NULL,
    table_nm_jpn TEXT
);

CREATE TABLE dataset_view_tables (
    physical_name TEXT NOT NULL,
    logical_name TEXT,
    nm TEXT NOT NULL,
    nm_jpn TEXT
);


create view v_view_table_field as
select vt.physical_name as view_physical_name, vt.logical_name as view_logical_name, vt.table_nm as table_physical_name, vt.table_nm_jpn as table_logical_name, tf.field as field, tf.field_jpn as field_jpn
from view_tables as vt 
left join table_fields as tf on vt.table_nm=tf.physical_name
order by vt.physical_name, vt.table_nm, tf.field;


create view v_dataset_view_table_field as
select dvf.physical_name as ds_physical_name, dvf.logical_name as ds_logical_name, dvf.nm as view_name, alv.table_physical_name as table_name, alv.field as field, alv.field_jpn as field_jpn 
from dataset_view_tables as dvf
left join 
(select * from v_view_table_field
union all 
select tf.physical_name as view_physical_name, tf.logical_name as view_logical_name, tf.physical_name as table_physical_name, tf.logical_name as table_logical_name, tf.field as field, tf.field_jpn as field_jpn 
from table_fields as tf) as alv on dvf.nm=alv.view_physical_name
order by dvf.physical_name, dvf.nm, alv.field;


--------------------------------------insert data------------------------------------------

--插入 table_fields 表数据

INSERT INTO table_fields (physical_name, logical_name, field, field_jpn) VALUES
('employees', '従業員', 'employee_id', '従業員ID'),
('employees', '従業員', 'first_name', '名'),
('employees', '従業員', 'last_name', '姓'),
('employees', '従業員', 'email', 'メール'),
('employees', '従業員', 'phone_number', '電話番号'),
('employees', '従業員', 'hire_date', '入社日'),
('employees', '従業員', 'job_title', '職位'),
('employees', '従業員', 'department_id', '部門ID'),
('employees', '従業員', 'salary', '給与'),
('employees', '従業員', 'manager_id', 'マネージャーID'),
('departments', '部門', 'department_id', '部門ID'),
('departments', '部門', 'department_name', '部門名'),
('departments', '部門', 'manager_id', 'マネージャーID'),
('departments', '部門', 'location', '所在地'),
('orders', '注文', 'order_id', '注文ID'),
('orders', '注文', 'customer_id', '顧客ID'),
('orders', '注文', 'order_date', '注文日'),
('orders', '注文', 'total_amount', '総額'),
('orders', '注文', 'status', 'ステータス'),
('orders', '注文', 'shipping_address', '配送先住所'),
('orders', '注文', 'payment_method', '支払方法'),
('accounts', '口座', 'account_id', '口座ID'),
('accounts', '口座', 'account_name', '口座名'),
('accounts', '口座', 'account_type', '口座タイプ'),
('accounts', '口座', 'balance', '残高'),
('accounts', '口座', 'created_date', '作成日'),
('accounts', '口座', 'status', 'ステータス'),
('system_logs', 'システムログ', 'log_id', 'ログID'),
('system_logs', 'システムログ', 'log_timestamp', 'ログタイムスタンプ'),
('system_logs', 'システムログ', 'log_level', 'ログレベル'),
('system_logs', 'システムログ', 'log_message', 'ログメッセージ'),
('system_logs', 'システムログ', 'user_id', 'ユーザーID'),
('system_logs', 'システムログ', 'ip_address', 'IPアドレス'),
('transactions', '取引', 'transaction_id', '取引ID'),
('transactions', '取引', 'account_id', '口座ID'),
('transactions', '取引', 'transaction_date', '取引日'),
('transactions', '取引', 'amount', '金額'),
('transactions', '取引', 'transaction_type', '取引タイプ'),
('transactions', '取引', 'description', '説明'),
('employees_history', '従業員履歴', 'history_id', '履歴ID'),
('employees_history', '従業員履歴', 'employee_id', '従業員ID'),
('employees_history', '従業員履歴', 'change_date', '変更日'),
('employees_history', '従業員履歴', 'old_job_title', '旧職位'),
('employees_history', '従業員履歴', 'new_job_title', '新職位'),
('employees_history', '従業員履歴', 'old_salary', '旧給与'),
('employees_history', '従業員履歴', 'new_salary', '新給与'),
('employees_history', '従業員履歴', 'changed_by', '変更者');


--插入 view_tables 表数据

INSERT INTO view_tables (physical_name, logical_name, table_nm, table_nm_jpn) VALUES
('v_employee_details', '従業員詳細', 'employees', '従業員'),
('v_employee_details', '従業員詳細', 'departments', '部門'),
('v_high_salary_employees', '高給与従業員', 'employees', '従業員'),
('v_active_accounts', '有効口座', 'accounts', '口座'),
('v_transaction_summary', '取引概要', 'transactions', '取引'),
('v_transaction_summary', '取引概要', 'accounts', '口座'),
('v_employee_history_log', '従業員履歴ログ', 'employees_history', '従業員履歴'),
('v_employee_history_log', '従業員履歴ログ', 'employees', '従業員');



--插入 dataset_view_tables 表数据
INSERT INTO dataset_view_tables (physical_name, logical_name, nm, nm_jpn) VALUES
('dataset_employee', '従業員データセット', 'employees', '従業員'),
('dataset_employee', '従業員データセット', 'v_employee_details', '従業員詳細'),
('dataset_account', '口座データセット', 'accounts', '口座'),
('dataset_account', '口座データセット', 'v_active_accounts', '有効口座'),
('dataset_transaction', '取引データセット', 'transactions', '取引'),
('dataset_transaction', '取引データセット', 'v_transaction_summary', '取引概要'),
('dataset_history', '履歴データセット', 'employees_history', '従業員履歴'),
('dataset_history', '履歴データセット', 'v_employee_history_log', '従業員履歴ログ');



CREATE SEQUENCE user_info_id_seq 
    INCREMENT 1 
    START 1 
    MINVALUE 1 
    MAXVALUE 9223372036854775807  -- PostgreSQL BIGINT最大值 
    CACHE 1;

-- Table: public.user_info

-- DROP TABLE IF EXISTS public.user_info;

CREATE TABLE IF NOT EXISTS public.user_info
(
    id integer NOT NULL DEFAULT nextval('user_info_id_seq'::regclass),
    user_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    password character varying(255) COLLATE pg_catalog."default" NOT NULL,
    role character varying(50) COLLATE pg_catalog."default" NOT NULL,
    phone character varying(15) COLLATE pg_catalog."default",
    isactive character varying(10) COLLATE pg_catalog."default" NOT NULL,
    comment text COLLATE pg_catalog."default",
    email character varying(100) COLLATE pg_catalog."default",
    username character varying(50) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT user_info_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.user_info
    OWNER to postgres;


INSERT INTO public.user_info(
	user_id, password, role, phone, isactive, comment, email, username)
	VALUES ( 'admin', '123', 'admin', '1234567890', 'active', 'admin', 'admin@takeda.com', 'admin');
INSERT INTO public.user_info(
	user_id, password, role, phone, isactive, comment, email, username)
	VALUES ( 'kb_manager', '123', 'kb_manager', '1234567890', 'active', 'kb_manager', 'kb_manager@takeda.com', 'kb_manager');
INSERT INTO public.user_info(
	user_id, password, role, phone, isactive, comment, email, username)
	VALUES ( 'user1', '123', 'user', '1234567890', 'active', 'user1', 'user1@takeda.com', 'user1');


CREATE TABLE chat_history (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    uuid VARCHAR(36) NOT NULL,
    sender VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    createDate TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP 
);

-- 用户 Alice 的聊天记录 
INSERT INTO chat_history (username, uuid, sender, message, createDate) 
VALUES ('Alice', '550e8400-e29b-41d4-a716-446655440000', 'user', '你好，我想查询我的订单状态', '2023-05-10 09:15:22+08');



CREATE TABLE postgre_doc_status (
    id SERIAL PRIMARY KEY,
    document_name VARCHAR(255) NOT NULL,
    document_type VARCHAR(50),
    process_status VARCHAR(20) DEFAULT 'pending',
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    uploader VARCHAR(100)
);


CREATE TABLE opensearch_doc_status (
    id SERIAL PRIMARY KEY,
    document_name VARCHAR(255) NOT NULL,
    document_type VARCHAR(50),
    process_status VARCHAR(20) DEFAULT 'pending',
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    uploader VARCHAR(100),
	index_name VARCHAR(100)
);