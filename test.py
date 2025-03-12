import requests
import json
import time
from requests.exceptions import ConnectionError, Timeout

# 准备表结构数据
company_schema = {
    "table_name": "m_company",
    "schema": "public",
    "data": [
        {
            "company_id": 1,
            "company_name": "Company A",
            "address": "Address A",
            "phone": "1234567890"
        },
        {
            "company_id": 2,
            "company_name": "Company B",
            "address": "Address B",
            "phone": "0987654321"
        }
    ]
}

distributor_schema = {
    "table_name": "m_distributor",
    "schema": "public",
    "data": [
        {
            "distributor_id": 1,
            "distributor_name": "Distributor A",
            "contact_info": "Contact A",
            "company_id": 1
        },
        {
            "distributor_id": 2,
            "distributor_name": "Distributor B",
            "contact_info": "Contact B",
            "company_id": 2
        }
    ]
}

def wait_for_server(url: str, max_retries: int = 5, delay: int = 2):
    """等待服务器启动"""
    for i in range(max_retries):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                print(f"Server is up and running after {i + 1} attempts")
                return True
        except Exception as e:
            print(f"Attempt {i + 1}/{max_retries}: Server not ready yet. Waiting {delay} seconds...")
            time.sleep(delay)
    return False

def main():
    base_url = "http://localhost:8000"
    
    # 等待服务器启动
    if not wait_for_server(f"{base_url}/docs"):
        print("Could not connect to server. Please make sure it's running.")
        return

    try:
        # 发送请求
        response = requests.post(
            f"{base_url}/database/schema/import",
            json={
                "schemas": [
                    json.dumps(company_schema),
                    json.dumps(distributor_schema)
                ],
                "description": "Import company and distributor tables"
            },
            timeout=30  # 设置30秒超时
        )

        # 检查响应
        print("\nStatus Code:", response.status_code)
        print("Response:", response.json())

    except Timeout:
        print("Request timed out. The server took too long to respond.")
    except ConnectionError as e:
        print(f"Connection error: {e}")
        print("Please check if the server is running and the port 8000 is available.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()