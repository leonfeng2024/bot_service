from fastapi import FastAPI, HTTPException, UploadFile, File, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from service.chat_service import ChatService
from service.llm_service import LLMService
from service.embedding_service import EmbeddingService
from service.neo4j_service import Neo4jService
from service.export_excel_service import ExportExcelService
from service.export_ppt_service import ExportPPTService
from models.models import ChatRequest, DatabaseSchemaRequest, TokenData, LogoutRequest
import logging
import logging.config
import os
import shutil
from typing import List
import jwt
import uuid
import time
from flask import request, jsonify
from tools.postgresql_tools import PostgreSQLTools
from tools.redis_tools import RedisTools
import json
from contextlib import asynccontextmanager
from config import JWT_SECRET, JWT_EXPIRATION

# 配置日志
import yaml
import os

# 确保logs目录存在
os.makedirs('logs', exist_ok=True)

# 加载日志配置
with open('logs/logging_config.yaml', 'r') as f:
    config = yaml.safe_load(f)
    logging.config.dictConfig(config)

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    yield
    # Shutdown logic
    neo4j_service.close()
    await export_service.close()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

chat_service = ChatService()
llm_service = LLMService()
neo4j_service = Neo4jService()
export_service = ExportExcelService()
ppt_service = ExportPPTService()
embedding_service = EmbeddingService()
redis_tools = RedisTools()

# 设置JWT密钥和过期时间(秒)
# JWT_SECRET = "your_jwt_secret_key"  # 请修改为安全的密钥
# JWT_EXPIRATION = 3600  # 1小时过期

# 添加中间件记录API调用
@app.middleware("http")
async def log_requests(request: Request, call_next):
    # 记录请求开始
    path = request.url.path
    method = request.method
    
    # 记录请求参数
    request_params = {}
    if method == "GET":
        request_params = dict(request.query_params)
    else:
        try:
            body = await request.body()
            if body:
                try:
                    # 尝试解析为JSON
                    request_params = json.loads(body)
                except:
                    request_params = {"raw_body": str(body)}
        except Exception as e:
            request_params = {"error": f"无法读取请求体: {str(e)}"}
    
    logger.info(f"API调用: {method} {path}, 参数: {request_params}")
    
    # 处理请求
    response = await call_next(request)
    
    # 记录响应
    logger.info(f"API完成: {method} {path}, 状态码: {response.status_code}")
    
    return response

# 添加 HTTP Bearer 认证
security = HTTPBearer()

async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        # 检查令牌是否过期
        if payload.get("exp") < int(time.time()):
            raise HTTPException(status_code=401, detail="Token has expired")
        
        return payload
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")

@app.post("/chat")
async def chat(request: ChatRequest, token_data: dict = Depends(verify_token)):
    logger.info(f"Received chat request - User: {request.username}, UUID: {request.uuid}")
    
    # 从请求中获取uuid并比较
    if request.uuid != token_data.get("uuid"):
        logger.warning(f"UUID mismatch - Request UUID: {request.uuid}, Token UUID: {token_data.get('uuid')}")
        raise HTTPException(status_code=403, detail="UUID mismatch")
    
    try:
        # 传递用户的UUID到chat_service
        response = await chat_service.handle_chat(request.username, request.query, request.uuid)
        logger.info(f"Chat request completed successfully - User: {request.username}, UUID: {request.uuid}")
        return response
    except Exception as e:
        logger.error(f"Error processing chat request - User: {request.username}, UUID: {request.uuid}, Error: {str(e)}")
        raise

@app.post("/logout")
async def logout(request: LogoutRequest, token_data: dict = Depends(verify_token)):
    """
    用户登出，清除Redis缓存
    """
    try:
        # 从请求中获取uuid
        uuid_to_delete = request.uuid
        logger.info(f"接收到登出请求，UUID: {uuid_to_delete}")
        
        # 删除Redis缓存
        success = redis_tools.delete(uuid_to_delete)
        
        if success:
            logger.info(f"成功删除用户缓存数据，UUID: {uuid_to_delete}")
            return {"message": "Logout successful", "status": "success"}
        else:
            logger.warning(f"未找到用户缓存数据，UUID: {uuid_to_delete}")
            return {"message": "No cache data found for user", "status": "warning"}
            
    except Exception as e:
        logger.error(f"登出过程中发生错误: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error during logout: {str(e)}"
        )

@app.post("/database/schema/import")
async def import_database_schema(request: DatabaseSchemaRequest):
    """
    Import database schema into Neo4j
    """
    try:
        # Import each schema
        for schema in request.schemas:
            success = await neo4j_service.import_table_schema(schema)
            if not success:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to import schema: {schema}"
                )
        
        # Create foreign key relationships after all schemas are imported
        success = await neo4j_service.create_foreign_key_relationships()
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to create foreign key relationships"
            )

        return {"message": "Successfully imported database schema", "status": "success"}

    except Exception as e:
        logger.error(f"Error importing database schema: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error importing database schema: {str(e)}"
        )

@app.post("/database/relationships/export")
async def export_relationships():
    """
    Export database relationships to Excel
    """
    try:
        success = await export_service.export_relationships_to_excel()
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to export relationships to Excel"
            )
        
        return {
            "message": "Successfully exported relationships to Excel",
            "status": "success",
            "file": "relationship.xlsx"
        }

    except Exception as e:
        logger.error(f"Error exporting relationships: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error exporting relationships: {str(e)}"
        )

@app.post("/database/relationships/export/ppt")
async def export_relationships_to_ppt():
    """
    Export database relationships to PPT
    """
    try:
        # First ensure the Excel file exists
        if not os.path.exists("relationship.xlsx"):
            raise HTTPException(
                status_code=404,
                detail="Relationships Excel file not found. Please export to Excel first."
            )
        
        output_file = "database_relationships.pptx"
        success = await ppt_service.create_ppt("relationship.xlsx", output_file)
        
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to create PPT presentation"
            )
        
        # Return the PPT file
        return FileResponse(
            output_file,
            media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
            filename=output_file
        )

    except Exception as e:
        logger.error(f"Error creating PPT presentation: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error creating PPT presentation: {str(e)}"
        )

@app.post("/file/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Upload a single file and save it to the upload_documents directory
    """
    try:
        # 验证文件类型
        allowed_types = ["text/plain", "application/pdf", "application/msword", 
                        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"]
        content_type = file.content_type
        if content_type not in allowed_types:
            raise HTTPException(
                status_code=400,
                detail=f"File type {content_type} not allowed. Allowed types: {allowed_types}"
            )
        
        # 验证文件大小（限制为10MB）
        MAX_SIZE = 10 * 1024 * 1024  # 10MB in bytes
        file_size = 0
        file_data = await file.read()
        file_size = len(file_data)
        
        if file_size > MAX_SIZE:
            raise HTTPException(
                status_code=400,
                detail=f"File size {file_size} bytes exceeds maximum allowed size of {MAX_SIZE} bytes"
            )
        
        # Create upload directory if it doesn't exist
        upload_dir = "upload_documents"
        os.makedirs(upload_dir, exist_ok=True)
        
        # Save the uploaded file
        # 使用字符串类型参数调用os.path.join
        file_path = os.path.join(str(upload_dir), str(file.filename))
        
        # Write the file
        with open(file_path, "wb") as buffer:
            buffer.write(file_data)
        
        return {"status": "success", "message": "done", "file_size": file_size}
    
    except Exception as e:
        logger.error(f"Error uploading file: {str(e)}")
        return {"status": "failed", "message": f"system error details: {str(e)}"}

@app.post("/file/upload/multiple")
async def upload_multiple_files(files: List[UploadFile] = File(...)):
    """
    Upload multiple files and save them to the upload_documents directory
    """
    try:
        # Create upload directory if it doesn't exist
        upload_dir = "upload_documents"
        os.makedirs(upload_dir, exist_ok=True)
        
        # Save all uploaded files
        for file in files:
            file_path = os.path.join(str(upload_dir), str(file.filename))
            
            # Write the file
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
        
        return {"status": "success", "message": "done"}
    
    except Exception as e:
        logger.error(f"Error uploading files: {str(e)}")
        return {"status": "failed", "message": f"system error details: {str(e)}"}

# 修改为FastAPI风格的路由，而不是Flask风格
@app.post('/token')
async def login(request: Request):
    logger.info("Login attempt")
    try:
        data = await request.json()
        username = data.get('username')
        password = data.get('password')
        logger.info(f"Login attempt - Username: {username}")
        print(f"Login attempt - Username: {username}")
        # 创建PostgreSQL工具并验证用户
        pg_tools = PostgreSQLTools()
        result = pg_tools.validate_user_credentials(username, password)
        print(result)
        # 验证失败
        if not result:
            logger.warning(f"Login failed - Invalid credentials for username: {username}")
            return {
                "status": "error",
                "message": "Invalid username or password",
                "code": 401
            }
            
        user_info = {
            'user_id': result['user_id'],
            'role': result['role']
        }
        
        # 生成唯一UUID
        user_uuid = str(uuid.uuid4())
        
        # 计算过期时间
        current_time = int(time.time())
        expiry_time = current_time + JWT_EXPIRATION
        
        # 生成JWT令牌
        payload = {
            "user_id": user_info['user_id'],
            "role": user_info['role'],
            "uuid": user_uuid,
            "exp": expiry_time
        }
        
        # 创建访问令牌
        access_token = jwt.encode(payload, JWT_SECRET, algorithm="HS256")
        
        # 创建刷新令牌 (通常有更长的过期时间)
        refresh_payload = {
            "user_id": user_info['user_id'],
            "uuid": user_uuid,
            "exp": current_time + (JWT_EXPIRATION * 24 * 7)  # 7天
        }
        refresh_token = jwt.encode(refresh_payload, JWT_SECRET, algorithm="HS256")
        
        # 在Redis中存储用户信息
        initial_data = {
            "user_id": user_info['user_id'],
            "role": user_info['role'],
            "username": username,
            "login_time": current_time
        }
        redis_tools.set(user_uuid, initial_data)
        
        logger.info(f"登录成功: 用户ID = {user_info['user_id']}, 角色 = {user_info['role']}")
        
        # 返回令牌信息
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expired_date": expiry_time,
            "uuid": user_uuid
        }
    except Exception as e:
        logger.error(f"登录过程中发生错误: {str(e)}")
        return {}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    
# uvicorn server:app --host 0.0.0.0 --port 8000 --reload