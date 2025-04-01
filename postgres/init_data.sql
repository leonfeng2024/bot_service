CREATE DATABASE local_rag
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LOCALE_PROVIDER = 'libc'
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
    
--- Master Table
--- employees 
CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY, -- 社員ID
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone_number VARCHAR(15),
    hire_date DATE NOT NULL,
    job_title VARCHAR(50) NOT NULL,
    department_id INT,
    salary DECIMAL(10, 2) NOT NULL,
    manager_id INT,
    CONSTRAINT fk_manager FOREIGN KEY (manager_id) REFERENCES employees(employee_id)
);

--- departments
CREATE TABLE departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL UNIQUE,
    manager_id INT,
    location VARCHAR(255),
    CONSTRAINT fk_department_manager FOREIGN KEY (manager_id) REFERENCES employees(employee_id)
);


--- orders
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL,
    shipping_address VARCHAR(255),
    payment_method VARCHAR(50)
);


--- accounts
CREATE TABLE accounts (
    account_id SERIAL PRIMARY KEY,
    account_name VARCHAR(100) NOT NULL,
    account_type VARCHAR(50) NOT NULL,
    balance DECIMAL(15, 2) NOT NULL,
    created_date DATE NOT NULL,
    status VARCHAR(50) NOT NULL 
);

--- system_logs
CREATE TABLE system_logs (
    log_id SERIAL PRIMARY KEY,
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    log_level VARCHAR(20) NOT NULL,
    log_message TEXT NOT NULL,
    user_id INT,
    ip_address VARCHAR(50)
);


--- transactions
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    account_id INT NOT NULL,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    amount DECIMAL(15, 2) NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    description TEXT,
    CONSTRAINT fk_account FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);

--- employees_history
CREATE TABLE employees_history (
    history_id SERIAL PRIMARY KEY,
    employee_id INT NOT NULL,
    change_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    old_job_title VARCHAR(50),
    new_job_title VARCHAR(50),
    old_salary DECIMAL(10, 2),
    new_salary DECIMAL(10, 2),
    changed_by INT,
    CONSTRAINT fk_employee FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
    CONSTRAINT fk_changed_by FOREIGN KEY (changed_by) REFERENCES employees(employee_id)
);






--- View
--- v_employee_details
--- 显示员工的详细信息，包括姓名、职位、部门名称和工资
CREATE VIEW v_employee_details AS 
SELECT 
    e.employee_id,  
    e.first_name  || ' ' || e.last_name  AS full_name, -- 使用 || 连接字符串 
    e.job_title,  
    d.department_name,  
    e.salary   
FROM 
    employees e 
LEFT JOIN 
    departments d ON e.department_id  = d.department_id; 



--- v_high_salary_employees
--- 显示工资高于 10000 的员工信息
CREATE VIEW v_high_salary_employees AS 
SELECT 
    employee_id,
    first_name || ' ' || last_name AS full_name, -- 使用 || 连接字符串 
    job_title,
    salary 
FROM 
    employees 
WHERE 
    salary > 10000;


--- v_active_accounts
--- 显示状态为“活跃”的账户信息
CREATE VIEW v_active_accounts AS 
SELECT 
    account_id,
    account_name,
    account_type,
    balance,
    created_date 
FROM 
    accounts 
WHERE 
    status = 'active';


--- v_transaction_summary
--- 显示每个账户的交易总金额和交易次数
CREATE VIEW v_transaction_summary AS 
SELECT 
    t.account_id,  
    a.account_name,  
    COUNT(t.transaction_id)  AS transaction_count,
    SUM(t.amount)  AS total_amount 
FROM 
    transactions t 
JOIN 
    accounts a ON t.account_id  = a.account_id   
GROUP BY 
    t.account_id,  a.account_name; 



--- v_employee_history_log
--- 显示员工历史变更的详细信息，包括变更时间、旧值和新值
CREATE VIEW v_employee_history_log AS 
SELECT 
    eh.history_id,  
    eh.employee_id,  
    e.first_name  || ' ' || e.last_name  AS employee_name, -- 使用 || 连接字符串 
    eh.change_date,  
    eh.old_job_title,  
    eh.new_job_title,  
    eh.old_salary,  
    eh.new_salary,  
    m.first_name  || ' ' || m.last_name  AS changed_by_name -- 使用 || 连接字符串 
FROM 
    employees_history eh 
JOIN 
    employees e ON eh.employee_id  = e.employee_id   
LEFT JOIN 
    employees m ON eh.changed_by  = m.employee_id; 













--- Procedure

--- p_GetEmployeeDetails
--- 根据员工 ID 获取员工的详细信息，包括姓名、职位、部门名称和工资
--- 结合了 employees 表和 v_employee_details 视图
CREATE OR REPLACE FUNCTION p_GetEmployeeDetails(emp_id INT)
RETURNS TABLE (
    employee_id INT,
    full_name TEXT,
    job_title VARCHAR(50),
    department_name VARCHAR(100),
    salary DECIMAL(10, 2)
) AS $$
BEGIN 
    RETURN QUERY 
    SELECT 
        ed.employee_id,  
        ed.full_name,  
        ed.job_title,  
        ed.department_name,  
        ed.salary  
    FROM 
        v_employee_details ed 
    WHERE 
        ed.employee_id  = p_GetEmployeeDetails.emp_id;  -- 明确指定参数来源 
END;
$$ LANGUAGE plpgsql;
--- 调用示例 SELECT * FROM p_GetEmployeeDetails(1);


--- p_GetAccountTransactions
--- 根据账户 ID 获取该账户的所有交易记录，并返回账户名称、交易类型、金额和交易时间
--- 结合了 transactions 表和 accounts 表
CREATE OR REPLACE FUNCTION p_GetAccountTransactions(acc_id INT)
RETURNS TABLE (
    transaction_id INT,
    account_name VARCHAR(100),
    transaction_type VARCHAR(50),
    amount DECIMAL(15, 2),
    transaction_date TIMESTAMP 
) AS $$
BEGIN 
    RETURN QUERY 
    SELECT 
        t.transaction_id,  
        a.account_name,  
        t.transaction_type,  
        t.amount,  
        t.transaction_date   
    FROM 
        transactions t 
    JOIN 
        accounts a ON t.account_id  = a.account_id   
    WHERE 
        t.account_id  = acc_id;
END;
$$ LANGUAGE plpgsql;
--- 调用示例 SELECT * FROM p_GetAccountTransactions(1);


--- p_UpdateEmployeeSalary
--- 更新员工的工资，并将变更记录插入到 employees_history 表中
--- 结合了 employees 表和 employees_history 表
CREATE OR REPLACE FUNCTION p_UpdateEmployeeSalary(
    emp_id INT,
    new_salary DECIMAL(10, 2),
    changed_by INT 
) RETURNS VOID AS $$
DECLARE 
    old_salary DECIMAL(10, 2);
BEGIN 
    -- 获取当前工资 
    SELECT salary INTO old_salary FROM employees WHERE employee_id = emp_id;
    
    -- 更新员工工资 
    UPDATE employees SET salary = new_salary WHERE employee_id = emp_id;
    
    -- 插入历史记录 
    INSERT INTO employees_history (
        employee_id,
        old_salary,
        new_salary,
        changed_by 
    ) VALUES (
        emp_id,
        old_salary,
        new_salary,
        changed_by 
    );
END;
$$ LANGUAGE plpgsql;
--- 调用示例 SELECT p_UpdateEmployeeSalary(1, 12000, 2);



--- Master Data

INSERT INTO employees (first_name, last_name, email, phone_number, hire_date, job_title, department_id, salary, manager_id) VALUES 
('John', 'Doe', 'john.doe@example.com',  '1234567890', '2020-01-15', 'Software Engineer', 1, 75000.00, NULL),
('Jane', 'Smith', 'jane.smith@example.com',  '2345678901', '2019-05-20', 'Project Manager', 2, 90000.00, 1),
('Alice', 'Johnson', 'alice.johnson@example.com',  '3456789012', '2021-03-10', 'Data Analyst', 3, 65000.00, 2),
('Bob', 'Brown', 'bob.brown@example.com',  '4567890123', '2018-11-05', 'Senior Developer', 1, 85000.00, 1),
('Charlie', 'Davis', 'charlie.davis@example.com',  '5678901234', '2022-07-01', 'HR Specialist', 4, 60000.00, 3),
('Eva', 'Wilson', 'eva.wilson@example.com',  '6789012345', '2020-09-12', 'Marketing Manager', 5, 95000.00, 2),
('Frank', 'Moore', 'frank.moore@example.com',  '7890123456', '2019-04-25', 'DevOps Engineer', 1, 80000.00, 1),
('Grace', 'Taylor', 'grace.taylor@example.com',  '8901234567', '2021-12-30', 'Financial Analyst', 6, 70000.00, 3),
('Henry', 'Anderson', 'henry.anderson@example.com',  '9012345678', '2023-02-15', 'Junior Developer', 1, 55000.00, 1),
('Ivy', 'Thomas', 'ivy.thomas@example.com',  '0123456789', '2022-08-20', 'Sales Representative', 7, 50000.00, 4);



INSERT INTO departments (department_name, manager_id, location) VALUES 
('Engineering', 1, 'New York'),
('Project Management', 2, 'San Francisco'),
('Data Science', 3, 'Chicago'),
('Human Resources', 4, 'Los Angeles'),
('Marketing', 5, 'Miami'),
('Finance', 6, 'Boston'),
('Sales', 7, 'Dallas'),
('Customer Support', 8, 'Seattle'),
('Product Management', 9, 'Austin'),
('Research and Development', 10, 'Denver');

INSERT INTO orders (customer_id, order_date, total_amount, status, shipping_address, payment_method) VALUES 
(1, '2025-01-10', 150.00, 'Shipped', '123 Main St, New York, NY', 'Credit Card'),
(2, '2025-01-15', 200.00, 'Processing', '456 Elm St, San Francisco, CA', 'PayPal'),
(3, '2025-02-01', 300.00, 'Delivered', '789 Oak St, Chicago, IL', 'Credit Card'),
(4, '2025-02-05', 100.00, 'Shipped', '321 Pine St, Los Angeles, CA', 'Debit Card'),
(5, '2025-02-10', 250.00, 'Processing', '654 Maple St, Miami, FL', 'PayPal'),
(6, '2025-03-01', 400.00, 'Delivered', '987 Cedar St, Boston, MA', 'Credit Card'),
(7, '2025-03-05', 150.00, 'Shipped', '123 Birch St, Dallas, TX', 'Debit Card'),
(8, '2025-03-10', 500.00, 'Processing', '456 Walnut St, Seattle, WA', 'PayPal'),
(9, '2025-03-12', 350.00, 'Delivered', '789 Cherry St, Austin, TX', 'Credit Card'),
(10, '2025-03-13', 200.00, 'Shipped', '321 Spruce St, Denver, CO', 'Debit Card');



INSERT INTO accounts (account_name, account_type, balance, created_date, status) VALUES 
('Savings Account', 'Savings', 10000.00, '2020-01-01', 'active'),
('Checking Account', 'Checking', 5000.00, '2020-02-01', 'active'),
('Investment Account', 'Investment', 20000.00, '2020-03-01', 'active'),
('Retirement Account', 'Retirement', 30000.00, '2020-04-01', 'active'),
('Business Account', 'Business', 15000.00, '2020-05-01', 'active'),
('Student Account', 'Savings', 1000.00, '2020-06-01', 'inactive'),
('Joint Account', 'Checking', 7000.00, '2020-07-01', 'active'),
('High-Yield Savings', 'Savings', 25000.00, '2020-08-01', 'active'),
('Credit Account', 'Credit', -500.00, '2020-09-01', 'active'),
('Trust Account', 'Trust', 100000.00, '2020-10-01', 'active');

INSERT INTO system_logs (log_level, log_message, user_id, ip_address) VALUES 
('INFO', 'User logged in', 1, '192.168.1.1'),
('WARN', 'Failed login attempt', 2, '192.168.1.2'),
('ERROR', 'Database connection failed', 3, '192.168.1.3'),
('INFO', 'Order created', 4, '192.168.1.4'),
('WARN', 'Low disk space', 5, '192.168.1.5'),
('ERROR', 'Payment processing failed', 6, '192.168.1.6'),
('INFO', 'User logged out', 7, '192.168.1.7'),
('WARN', 'Invalid input detected', 8, '192.168.1.8'),
('ERROR', 'System crash', 9, '192.168.1.9'),
('INFO', 'Backup completed', 10, '192.168.1.10');

INSERT INTO transactions (account_id, amount, transaction_type, description) VALUES 
(1, 500.00, 'Deposit', 'Salary deposit'),
(2, 200.00, 'Withdrawal', 'ATM withdrawal'),
(3, 1000.00, 'Transfer', 'Investment transfer'),
(4, 500.00, 'Deposit', 'Retirement contribution'),
(5, 300.00, 'Withdrawal', 'Business expense'),
(6, 100.00, 'Deposit', 'Student allowance'),
(7, 500.00, 'Transfer', 'Joint account transfer'),
(8, 2000.00, 'Deposit', 'High-yield savings'),
(9, 50.00, 'Withdrawal', 'Credit payment'),
(10, 10000.00, 'Transfer', 'Trust fund transfer');

INSERT INTO employees_history (employee_id, old_job_title, new_job_title, old_salary, new_salary, changed_by) VALUES 
(1, 'Software Engineer', 'Senior Software Engineer', 75000.00, 85000.00, 2),
(2, 'Project Manager', 'Senior Project Manager', 90000.00, 100000.00, 3),
(3, 'Data Analyst', 'Senior Data Analyst', 65000.00, 75000.00, 1),
(4, 'Senior Developer', 'Tech Lead', 85000.00, 95000.00, 2),
(5, 'HR Specialist', 'HR Manager', 60000.00, 70000.00, 4),
(6, 'Marketing Manager', 'Senior Marketing Manager', 95000.00, 105000.00, 2),
(7, 'DevOps Engineer', 'Senior DevOps Engineer', 80000.00, 90000.00, 1),
(8, 'Financial Analyst', 'Senior Financial Analyst', 70000.00, 80000.00, 3),
(9, 'Junior Developer', 'Software Engineer', 55000.00, 65000.00, 1),
(10, 'Sales Representative', 'Senior Sales Representative', 50000.00, 60000.00, 4);






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