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
CREATE OR REPLACE FUNCTION p_p_UpdateEmployeeSalary(
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