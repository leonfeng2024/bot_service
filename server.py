from fastapi import FastAPI, HTTPException, UploadFile, File, Request, Depends, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from service.chat_service import ChatService
from service.llm_service import LLMService
from service.embedding_service import EmbeddingService
from service.neo4j_service import Neo4jService
from service.export_excel_service import ExportExcelService
from service.export_ppt_service import ExportPPTService
from service.user_service import UserService
from service.postgres_service import PostgresService
from service.opensearch_service import OpenSearchService
from service.opensearch_procedure_service import OpenSearchProcedureService
from models.models import ChatRequest, DatabaseSchemaRequest, TokenData, LogoutRequest
from models.user_models import CreateUserRequest, UpdateUserRequest, UserProfileResponse, UserResponseWithMessage, DeleteUserResponse
from models.postgres_models import TableInfo, ExecuteQueryRequest, ExecuteQueryResponse, ErrorResponse, ImportResponse
from models.opensearch_models import IndexInfo, CreateIndexRequest, GenericResponse, SearchRequest, SearchResponse
import logging
import logging.config
import os
import shutil
from typing import List, Dict, Any
import jwt
import uuid
import time
from flask import request, jsonify
from tools.opensearch_tools import OpenSearchTools
from tools.postgresql_tools import PostgreSQLTools
from tools.redis_tools import RedisTools
from tools.ddl_to_postgre import process_ddl_file
from tools.excel_to_postgre import process_excel_file
import json
import asyncio
from contextlib import asynccontextmanager
from config import JWT_SECRET, JWT_EXPIRATION
import re

# Configure logging
import yaml
import os

# Ensure logs directory exists
os.makedirs('logs', exist_ok=True)

# Load logging configuration
with open('logs/logging_config.yaml', 'r') as f:
    config = yaml.safe_load(f)
    logging.config.dictConfig(config)

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    try:
        # Ensure the postgre_doc_status table exists
        await ensure_doc_status_table()
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}")
    
    yield
    # Shutdown logic
    neo4j_service.close()
    await export_service.close()

async def ensure_doc_status_table():
    """Ensure the postgre_doc_status table exists in the database"""
    try:
        # Check if the table exists
        check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'postgre_doc_status'
        ) as exists
        """
        
        result = await postgres_service.execute_query(check_query)
        table_exists = result[0]['exists'] if result else False
        
        if not table_exists:
            logger.info("Creating postgre_doc_status table")
            
            # Create the table
            create_query = """
            CREATE TABLE postgre_doc_status (
                id SERIAL PRIMARY KEY,
                document_name VARCHAR(255) NOT NULL,
                document_type VARCHAR(50),
                process_status VARCHAR(20) DEFAULT 'In Process',
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                uploader VARCHAR(100)
            )
            """
            
            await postgres_service.execute_query(create_query)
            logger.info("postgre_doc_status table created successfully")
    
    except Exception as e:
        logger.error(f"Error ensuring postgre_doc_status table exists: {str(e)}")
        raise

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the output directory as a static files directory
output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
os.makedirs(output_dir, exist_ok=True)
app.mount("/output", StaticFiles(directory=output_dir), name="output")

chat_service = ChatService()
llm_service = LLMService()
neo4j_service = Neo4jService()
export_service = ExportExcelService()
ppt_service = ExportPPTService()
embedding_service = EmbeddingService()
redis_tools = RedisTools()
user_service = UserService()
postgres_service = PostgresService()
opensearch_service = OpenSearchService()
opensearch_procedure_service = OpenSearchProcedureService()

# Add middleware to log API calls
@app.middleware("http")
async def log_requests(request: Request, call_next):
    # Log request start
    path = request.url.path
    method = request.method
    
    # Log request parameters
    request_params = {}
    if method == "GET":
        request_params = dict(request.query_params)
    else:
        try:
            body = await request.body()
            if body:
                try:
                    # Try to parse as JSON
                    request_params = json.loads(body)
                except:
                    request_params = {"raw_body": str(body)}
        except Exception as e:
            request_params = {"error": f"Unable to read request body: {str(e)}"}
    
    logger.info(f"API Call: {method} {path}, Parameters: {request_params}")
    
    # Process request
    response = await call_next(request)
    
    # Log response
    logger.info(f"API Complete: {method} {path}, Status Code: {response.status_code}")
    
    return response

# Add HTTP Bearer authentication
security = HTTPBearer()

async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        
        # Allow dev token to bypass verification
        if token == "DEVTOKEN":
            logger.warning("Development token used to bypass authentication")
            return {
                "user_id": 999,
                "role": "admin",
                "uuid": "dev-uuid",
                "exp": int(time.time()) + JWT_EXPIRATION
            }
            
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        # Check if token has expired
        if payload.get("exp") < int(time.time()):
            raise HTTPException(status_code=401, detail="Token has expired")
        
        return payload
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")

@app.post("/chat")
async def chat(request: ChatRequest, token_data: dict = Depends(verify_token)):
    logger.info(f"Received chat request - User: {request.username}, UUID: {request.uuid}")
    
    # Get UUID from request and compare
    if request.uuid != token_data.get("uuid"):
        logger.warning(f"UUID mismatch - Request UUID: {request.uuid}, Token UUID: {token_data.get('uuid')}")
        raise HTTPException(status_code=403, detail="UUID mismatch")
    
    try:
        # Use generator function for streaming response
        async def generate_response():
            async for chunk in chat_service.handle_chat(request.username, request.query, request.uuid):
                # Convert each chunk to JSON string and add newline
                yield json.dumps(chunk) + "\n"
        
        return StreamingResponse(
            generate_response(),
            media_type="application/json"
        )
    except Exception as e:
        logger.error(f"Error processing chat request - User: {request.username}, UUID: {request.uuid}, Error: {str(e)}")
        raise

@app.post("/logout")
async def logout(request: LogoutRequest, token_data: dict = Depends(verify_token)):
    """
    User logout, clear Redis cache
    """
    try:
        # Get UUID from request
        uuid_to_delete = request.uuid
        logger.info(f"Received logout request, UUID: {uuid_to_delete}")
        
        # Delete Redis cache
        success = redis_tools.delete(uuid_to_delete)
        
        if success:
            logger.info(f"Successfully deleted user cache data, UUID: {uuid_to_delete}")
            return {"message": "Logout successful", "status": "success"}
        else:
            logger.warning(f"No cache data found for user, UUID: {uuid_to_delete}")
            return {"message": "No cache data found for user", "status": "warning"}
            
    except Exception as e:
        logger.error(f"Error during logout: {str(e)}")
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
        # Validate file type
        allowed_types = ["text/plain", "application/pdf", "application/msword", 
                        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"]
        content_type = file.content_type
        if content_type not in allowed_types:
            raise HTTPException(
                status_code=400,
                detail=f"File type {content_type} not allowed. Allowed types: {allowed_types}"
            )
        
        # Validate file size (limit to 10MB)
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

@app.post('/token')
async def login(request: Request):
    logger.info("Login attempt")
    try:
        data = await request.json()
        username = data.get('username')
        password = data.get('password')
        logger.info(f"Login attempt - Username: {username}")
        print(f"Login attempt - Username: {username}")
        # Create PostgreSQL tools and validate user
        pg_tools = PostgreSQLTools()
        result = pg_tools.validate_user_credentials(username, password)
        print(result)
        # Validation failed
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
        
        # Generate unique UUID
        user_uuid = str(uuid.uuid4())
        
        # Calculate expiration time
        current_time = int(time.time())
        expiry_time = current_time + JWT_EXPIRATION
        
        # Generate JWT token
        payload = {
            "user_id": user_info['user_id'],
            "role": user_info['role'],
            "uuid": user_uuid,
            "exp": expiry_time
        }
        
        # Create access token
        access_token = jwt.encode(payload, JWT_SECRET, algorithm="HS256")
        
        # Create refresh token (usually has longer expiration time)
        refresh_payload = {
            "user_id": user_info['user_id'],
            "uuid": user_uuid,
            "exp": current_time + (JWT_EXPIRATION * 24 * 7)  # 7 days
        }
        refresh_token = jwt.encode(refresh_payload, JWT_SECRET, algorithm="HS256")
        
        # Store user information in Redis
        initial_data = {
            "user_id": user_info['user_id'],
            "role": user_info['role'],
            "username": username,
            "login_time": current_time
        }
        redis_tools.set(user_uuid, initial_data)
        
        logger.info(f"Login successful: User ID = {user_info['user_id']}, Role = {user_info['role']}")
        
        # Return token information
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expired_date": expiry_time,
            "uuid": user_uuid
        }
    except Exception as e:
        logger.error(f"Error during login: {str(e)}")
        return {}

@app.get("/user/profile", response_model=UserProfileResponse)
async def get_user_profile(token_data: dict = Depends(verify_token)):
    """
    Get current user's profile information
    """
    try:
        user_id = token_data.get("user_id")
        if not user_id:
            raise HTTPException(status_code=400, detail="User ID not found in token")
        
        user_profile = await user_service.get_user_profile(user_id)
        
        if not user_profile:
            raise HTTPException(status_code=404, detail="User not found")
        
        return user_profile
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error getting user profile: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting user profile: {str(e)}"
        )

@app.get("/admin/users")
async def get_all_users(token_data: dict = Depends(verify_token)):
    """
    Get all users (admin only)
    """
    try:
        # Check if user has admin role
        if token_data.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin role required")
        
        users = await user_service.get_all_users()
        return users
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error getting all users: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting all users: {str(e)}"
        )

@app.post("/admin/users", response_model=UserResponseWithMessage)
async def create_user(request: CreateUserRequest, token_data: dict = Depends(verify_token)):
    """
    Create a new user (admin only)
    """
    try:
        # Check if user has admin role
        if token_data.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin role required")
        
        result = await user_service.create_user(
            username=request.username,
            email=request.email,
            password=request.password,
            role=request.role
        )
        
        if not result["success"]:
            status_code = 400 if "exists" in result["message"] else 500
            raise HTTPException(status_code=status_code, detail=result["message"])
        
        return result
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error creating user: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error creating user: {str(e)}"
        )

@app.put("/admin/users/{user_id}", response_model=UserResponseWithMessage)
async def update_user(
    user_id: str, 
    request: UpdateUserRequest, 
    token_data: dict = Depends(verify_token)
):
    """
    Update an existing user (admin only)
    """
    try:
        # Check if user has admin role
        if token_data.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin role required")
        
        # Convert Pydantic model to dict, excluding None values
        update_data = request.dict(exclude_none=True)
        
        result = await user_service.update_user(user_id, update_data)
        
        if not result["success"]:
            status_code = 404 if "does not exist" in result["message"] else 500
            raise HTTPException(status_code=status_code, detail=result["message"])
        
        return result
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error updating user: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error updating user: {str(e)}"
        )

@app.delete("/admin/users/{user_id}", response_model=DeleteUserResponse)
async def delete_user(user_id: str, token_data: dict = Depends(verify_token)):
    """
    Delete a user (admin only)
    """
    try:
        # Check if user has admin role
        if token_data.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin role required")
        
        result = await user_service.delete_user(user_id)
        
        if not result["success"]:
            status_code = 404 if "does not exist" in result["message"] else 500
            raise HTTPException(status_code=status_code, detail=result["message"])
        
        return result
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error deleting user: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting user: {str(e)}"
        )

@app.get("/postgres/tables", response_model=List[TableInfo])
async def get_postgres_tables(token_data: dict = Depends(verify_token)):
    """
    Get all tables from PostgreSQL database
    """
    try:
        # Check permissions (optional, depends on requirements)
        if token_data.get("role") not in ["admin", "kb_manager"]:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        
        # Get table list
        tables = await postgres_service.get_tables()
        return tables
    except Exception as e:
        logger.error(f"Error getting PostgreSQL tables: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting PostgreSQL tables: {str(e)}"
        )

@app.post("/postgres/execute", response_model=ExecuteQueryResponse)
async def execute_postgres_query(request: ExecuteQueryRequest, token_data: dict = Depends(verify_token)):
    """
    Execute SQL query and return results
    """
    try:
        # Check permissions (optional, depends on requirements)
        if token_data.get("role") not in ["admin", "kb_manager"]:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        
        # Execute query
        results = await postgres_service.execute_query(request.query)
        return {"rows": results}
    except Exception as e:
        logger.error(f"Error executing PostgreSQL query: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error executing PostgreSQL query: {str(e)}"
        )

@app.post("/postgres/import", response_model=ImportResponse)
async def import_postgres_data(file: UploadFile = File(...), token_data: dict = Depends(verify_token)):
    """
    Import data from SQL file to PostgreSQL
    """
    try:        
        # Check file type
        if not file.filename.endswith('.sql'):
            raise HTTPException(status_code=400, detail="Only SQL files are allowed")
        
        # Read file content
        file_content = await file.read()
        
        # Import data
        result = await postgres_service.import_data(file_content)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error importing data to PostgreSQL: {str(e)}")
        return {
            "success": False,
            "message": f"Error importing data: {str(e)}"
        }

@app.get("/postgres/export/{table_name}")
async def export_postgres_data(table_name: str, token_data: dict = Depends(verify_token)):
    """
    Export PostgreSQL table data to CSV file
    """
    try:
        # Check permissions (optional, depends on requirements)
        if token_data.get("role") not in ["admin", "kb_manager"]:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        
        # Export data
        file_path = await postgres_service.export_data(table_name)
        
        if not file_path:
            raise HTTPException(status_code=404, detail=f"No data found for table: {table_name}")
        
        # Return CSV file
        return FileResponse(
            file_path,
            media_type="text/csv",
            filename=f"{table_name}.csv"
        )
    except HTTPException:
        raise
    except ValueError as e:
        # Handle invalid table name error
        logger.error(f"Invalid table name: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error exporting data from PostgreSQL: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error exporting data: {str(e)}"
        )

@app.get("/opensearch/indices", response_model=List[IndexInfo])
async def get_opensearch_indices(token_data: dict = Depends(verify_token)):
    """
    Get all indices from OpenSearch
    """
    try:
        indices = await opensearch_service.get_indices()
        return indices
    except Exception as e:
        error_message = str(e)
        status_code = 500
        
        # Handle connection errors
        if "ConnectionError" in error_message or "connection" in error_message.lower():
            status_code = 503  # Service Unavailable
            error_message = "OpenSearch service unavailable, please check connection configuration or service status"
            
        logger.error(f"Error getting OpenSearch indices: {error_message}")
        raise HTTPException(
            status_code=status_code,
            detail=error_message
        )

@app.post("/opensearch/indices", response_model=GenericResponse)
async def create_opensearch_index(request: CreateIndexRequest, token_data: dict = Depends(verify_token)):
    """
    Create new OpenSearch index
    """
    try:
        result = await opensearch_service.create_index(request.index)
        if not result['success']:
            raise HTTPException(
                status_code=400,
                detail=result['message']
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating OpenSearch index: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error creating OpenSearch index: {str(e)}"
        )

@app.delete("/opensearch/indices/{index_name}", response_model=GenericResponse)
async def delete_opensearch_index(index_name: str, token_data: dict = Depends(verify_token)):
    """
    Delete OpenSearch index
    """
    try:
        result = await opensearch_service.delete_index(index_name)
        if not result['success']:
            raise HTTPException(
                status_code=404 if "does not exist" in result['message'] else 400,
                detail=result['message']
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting OpenSearch index: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting OpenSearch index: {str(e)}"
        )

@app.post("/opensearch/search", response_model=SearchResponse)
async def search_opensearch(request: SearchRequest, token_data: dict = Depends(verify_token)):
    """
    Execute search in OpenSearch index
    """
    try:
        result = await opensearch_service.search(request.index, request.query)
        return result
    except Exception as e:
        logger.error(f"Error searching OpenSearch: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error searching OpenSearch: {str(e)}"
        )

@app.post("/opensearch/upload", response_model=GenericResponse)
async def upload_to_opensearch(
    file: UploadFile = File(...),
    index: str = Form(...),
    token_data: dict = Depends(verify_token)
):
    """
    Upload document to OpenSearch index
    """
    try:
        # Check file type
        allowed_types = ['.txt', '.doc', '.docx', '.xls', '.xlsx']
        file_ext = os.path.splitext(file.filename)[1].lower()
        
        if file_ext not in allowed_types:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type. Allowed types: {', '.join(allowed_types)}"
            )
        
        # Read file content
        file_content = await file.read()
        
        # Upload document
        result = await opensearch_service.upload_document(index, file_content, file.filename)
        
        if not result['success']:
            raise HTTPException(
                status_code=400,
                detail=result['message']
            )
            
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading document to OpenSearch: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error uploading document to OpenSearch: {str(e)}"
        )

@app.post("/kb/sql/upload")
async def upload_sql_file(
    file: UploadFile = File(...),
    token_data: dict = Depends(verify_token),
    uploader: str = Form(None)
):
    """
    Upload and process SQL file for knowledge base
    
    Args:
        file: SQL file to upload
        token_data: User token data
        uploader: Name of the user who uploaded the file (optional)
        
    Returns:
        Response with status and message
    """
    try:
        # Validate file type
        if not file.filename.endswith('.sql'):
            raise HTTPException(
                status_code=400,
                detail="Only SQL files are allowed"
            )
        
        # Create kb_document directory if it doesn't exist
        kb_dir = "kb_document"
        os.makedirs(kb_dir, exist_ok=True)
        
        # Save the uploaded file
        file_path = os.path.join(kb_dir, file.filename)
        file_content = await file.read()
        
        # Write the file (overwrite if exists)
        with open(file_path, "wb") as buffer:
            buffer.write(file_content)
        
        # Get username from token or form
        username = uploader
        if not username and token_data:
            # Try to get user ID from token and convert to username
            user_id = token_data.get("user_id")
            if user_id:
                try:
                    user_profile = await user_service.get_user_profile(user_id)
                    username = user_profile.get("username", str(user_id))
                except:
                    username = str(user_id)
        
        # Record status in PostgreSQL
        document_type = "SQL"
        insert_query = """
        INSERT INTO postgre_doc_status 
        (document_name, document_type, process_status, uploader)
        VALUES (:document_name, :document_type, :process_status, :uploader)
        RETURNING id
        """
        
        result = await postgres_service.execute_query(
            insert_query, 
            {
                "document_name": file.filename,
                "document_type": document_type,
                "process_status": "In Process",
                "uploader": username
            }
        )
        
        # Get the document ID
        doc_id = result[0]['id'] if result and len(result) > 0 else None
        
        # Start background task to process the file
        asyncio.create_task(process_sql_file_background(file_path, doc_id))
        
        return {"status": "success", "message": "file upload success.start process kb document"}
        
    except Exception as e:
        logger.error(f"Error uploading SQL file: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error uploading SQL file: {str(e)}"
        )

async def process_sql_file_background(file_path, doc_id):
    """
    Background task to process SQL file
    
    Args:
        file_path: Path to the SQL file
        doc_id: Document ID in postgre_doc_status table
    """
    try:
        logger.info(f"Processing SQL file: {file_path}")
        
        # Get database URI from PostgreSQL tools
        pg_tools = PostgreSQLTools()
        db_uri = pg_tools.get_sqlalchemy_uri()
        
        # Process the DDL file
        process_ddl_file(file_path, db_uri)
        
        # Update status to "Completed"
        if doc_id:
            update_query = """
            UPDATE postgre_doc_status 
            SET process_status = 'Completed' 
            WHERE id = :id
            """
            
            await postgres_service.execute_query(
                update_query,
                {"id": doc_id}
            )
            
        logger.info(f"SQL file processing completed: {file_path}")
        
    except Exception as e:
        logger.error(f"Error processing SQL file {file_path}: {str(e)}")
        # Update status to "Failed" if there's an error
        if doc_id:
            try:
                update_query = """
                UPDATE postgre_doc_status 
                SET process_status = 'Failed' 
                WHERE id = :id
                """
                
                await postgres_service.execute_query(
                    update_query,
                    {"id": doc_id}
                )
            except Exception as update_error:
                logger.error(f"Error updating status: {str(update_error)}")

@app.get("/kb/doc/status")
async def get_document_status(token_data: dict = Depends(verify_token)):
    """
    Get status of all document processing records
    
    Returns:
        List of document status records
    """
    try:
        query = """
        SELECT 
            id,
            document_name,
            document_type,
            process_status,
            upload_date,
            uploader
        FROM 
            postgre_doc_status
        ORDER BY 
            upload_date DESC
        """
        
        result = await postgres_service.execute_query(query)
        return result
        
    except Exception as e:
        logger.error(f"Error getting document status: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting document status: {str(e)}"
        )

@app.delete("/kb/dataset/delete")
async def delete_dataset(document_name: str, token_data: dict = Depends(verify_token)):
    """
    Delete dataset from dataset_view_tables based on document name
    
    Args:
        document_name: Document name (e.g. "PD003_01マスタ_施設マスタ.xlsx" or "some_file.sql")
        
    Returns:
        Response with status and message
    """
    try:
        # Check file extension to determine processing logic
        file_extension = os.path.splitext(document_name)[1].lower()
        
        if file_extension in ['.xlsx', '.xls']:
            # Excel file processing
            # Extract dataset name from document name following the same logic in excel_to_postgre.py
            _file_name_list = document_name.split(".")[0].split("_")
            if len(_file_name_list) >= 3:
                dataset_name = _file_name_list[0] + "_" + _file_name_list[2]
            else:
                # Fallback if file name doesn't match expected format
                dataset_name = document_name.split(".")[0]
            
            # Delete from dataset_view_tables
            delete_query = """
            DELETE FROM dataset_view_tables
            WHERE physical_name = :physical_name
            """
            
            await postgres_service.execute_query(
                delete_query,
                {"physical_name": dataset_name}
            )
            
            logger.info(f"Deleted Excel dataset {dataset_name} from dataset_view_tables")
            
        elif file_extension == '.sql':
            # SQL file processing
            # Placeholder for future implementation
            await delete_sql_dataset(document_name)
            logger.info(f"Processed SQL file deletion: {document_name}")
            
        else:
            # Unsupported file type
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type: {file_extension}. Only Excel and SQL files are supported."
            )
        
        return {
            "status": "success", 
            "message": "Document data delete complete"
        }
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error deleting dataset: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting dataset: {str(e)}"
        )

async def delete_sql_dataset(document_name: str):
    """
    Delete dataset created from SQL file
    
    Args:
        document_name: SQL file name
        
    Returns:
        None
    """
    # Placeholder for future implementation
    # This method will be implemented in the future to handle SQL file dataset deletion
    pass

@app.post("/kb/dataset/delete")
async def delete_dataset_data(request: Request, token_data: dict = Depends(verify_token)):
    """
    Delete dataset from dataset_view_tables and related record from postgre_doc_status
    
    Request body format:
        {
            "id": "8", 
            "document_name": "PD003_01マスタ_施設マスタ.xlsx"
        }
        
    Returns:
        Response with status and message
    """
    try:
        # Parse JSON request body
        data = await request.json()
        document_name = data.get("document_name")
        doc_id = data.get("id")
        
        if not document_name:
            raise HTTPException(
                status_code=400,
                detail="Missing required parameter: document_name"
            )
            
        if not doc_id:
            raise HTTPException(
                status_code=400,
                detail="Missing required parameter: id"
            )
        
        # Check file extension to determine processing logic
        file_extension = os.path.splitext(document_name)[1].lower()
        
        if file_extension in ['.xlsx', '.xls']:
            # Excel file processing
            # Extract dataset name from document name following the same logic in excel_to_postgre.py
            _file_name_list = document_name.split(".")[0].split("_")
            if len(_file_name_list) >= 3:
                dataset_name = _file_name_list[0] + "_" + _file_name_list[2]
            else:
                # Fallback if file name doesn't match expected format
                dataset_name = document_name.split(".")[0]
            
            # Delete from dataset_view_tables
            delete_query = """
            DELETE FROM dataset_view_tables
            WHERE physical_name = :physical_name
            """
            
            await postgres_service.execute_query(
                delete_query,
                {"physical_name": dataset_name}
            )
            
            logger.info(f"Deleted Excel dataset {dataset_name} from dataset_view_tables")
            
        elif file_extension == '.sql':
            # SQL file processing
            # Placeholder for future implementation
            await delete_sql_dataset(document_name)
            logger.info(f"Processed SQL file deletion: {document_name}")
            
        else:
            # Unsupported file type
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type: {file_extension}. Only Excel and SQL files are supported."
            )
        
        # Delete record from postgre_doc_status
        delete_status_query = """
        DELETE FROM postgre_doc_status
        WHERE id = :id
        """
        
        await postgres_service.execute_query(
            delete_status_query,
            {"id": doc_id}
        )
        
        logger.info(f"Deleted document status record with ID {doc_id}")
        
        return {
            "status": "success", 
            "message": "Document data delete complete"
        }
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error deleting dataset: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting dataset: {str(e)}"
        )

@app.post("/kb/excel/upload")
async def upload_excel_file(
    file: UploadFile = File(...),
    token_data: dict = Depends(verify_token),
    uploader: str = Form(None),
    sheet_name: str = Form("資材一覧"),
    header_row: int = Form(2),
    table_col_name: str = Form("テーブル名")
):
    """
    Upload and process Excel file for knowledge base
    
    Args:
        file: Excel file to upload
        token_data: User token data
        uploader: Name of the user who uploaded the file (optional)
        sheet_name: Name of the sheet containing table info
        header_row: Row number containing headers (1-based)
        table_col_name: Column name containing table names
        
    Returns:
        Response with status and message
    """
    try:
        # Validate file type
        file_extension = os.path.splitext(file.filename)[1].lower()
        if file_extension not in ['.xlsx', '.xls']:
            raise HTTPException(
                status_code=400,
                detail="Only Excel files (.xlsx, .xls) are allowed"
            )
        
        # Create kb_document directory if it doesn't exist
        kb_dir = "kb_document"
        os.makedirs(kb_dir, exist_ok=True)
        
        # Save the uploaded file
        file_path = os.path.join(kb_dir, file.filename)
        file_content = await file.read()
        
        # Write the file (overwrite if exists)
        with open(file_path, "wb") as buffer:
            buffer.write(file_content)
        
        # Get username from token or form
        username = uploader
        if not username and token_data:
            # Try to get user ID from token and convert to username
            user_id = token_data.get("user_id")
            if user_id:
                try:
                    user_profile = await user_service.get_user_profile(user_id)
                    username = user_profile.get("username", str(user_id))
                except:
                    username = str(user_id)
        
        # Record status in PostgreSQL
        document_type = "Excel"
        insert_query = """
        INSERT INTO postgre_doc_status 
        (document_name, document_type, process_status, uploader)
        VALUES (:document_name, :document_type, :process_status, :uploader)
        RETURNING id
        """
        
        result = await postgres_service.execute_query(
            insert_query, 
            {
                "document_name": file.filename,
                "document_type": document_type,
                "process_status": "In Process",
                "uploader": username
            }
        )
        
        # Get the document ID
        doc_id = result[0]['id'] if result and len(result) > 0 else None
        
        # Start background task to process the file
        asyncio.create_task(process_excel_file_background(
            file_path, 
            doc_id, 
            sheet_name, 
            header_row, 
            table_col_name
        ))
        
        return {"status": "success", "message": "file upload success.start process kb document"}
        
    except Exception as e:
        logger.error(f"Error uploading Excel file: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error uploading Excel file: {str(e)}"
        )

async def process_excel_file_background(file_path, doc_id, sheet_name, header_row, table_col_name):
    """
    Background task to process Excel file
    
    Args:
        file_path: Path to the Excel file
        doc_id: Document ID in postgre_doc_status table
        sheet_name: Name of the sheet containing table info
        header_row: Row number containing headers (1-based)
        table_col_name: Column name containing table names
    """
    try:
        logger.info(f"Processing Excel file: {file_path}")
        
        # Get database URI from PostgreSQL tools
        pg_tools = PostgreSQLTools()
        db_uri = pg_tools.get_sqlalchemy_uri()
        
        # Process the Excel file
        dataset_name, table_set = process_excel_file(
            file_path, 
            db_uri, 
            sheet_name, 
            header_row, 
            table_col_name
        )
        
        logger.info(f"Extracted dataset: {dataset_name} with {len(table_set)} tables")
        
        # Update status to "Completed"
        if doc_id:
            update_query = """
            UPDATE postgre_doc_status 
            SET process_status = 'Completed' 
            WHERE id = :id
            """
            
            await postgres_service.execute_query(
                update_query,
                {"id": doc_id}
            )
            
        logger.info(f"Excel file processing completed: {file_path}")
        
    except Exception as e:
        logger.error(f"Error processing Excel file {file_path}: {str(e)}")
        # Update status to "Failed" if there's an error
        if doc_id:
            try:
                update_query = """
                UPDATE postgre_doc_status 
                SET process_status = 'Failed' 
                WHERE id = :id
                """
                
                await postgres_service.execute_query(
                    update_query,
                    {"id": doc_id}
                )
            except Exception as update_error:
                logger.error(f"Error updating status: {str(update_error)}")

@app.post("/kb/openserch/upload")
async def upload_opensearch_procedure(
    file: UploadFile = File(...),
    token_data: dict = Depends(verify_token),
    uploader: str = Form(None),
    process_index: str = Form("procedure_index")
):
    """
    Upload and process SQL file for OpenSearch procedure indexing
    
    Args:
        file: SQL file to upload
        token_data: User token data
        uploader: Name of the user who uploaded the file (optional)
        process_index: Name of the OpenSearch index to use (default: procedure_index)
        
    Returns:
        Response with status and message
    """
    try:
        # Validate file type
        if not file.filename.endswith('.sql'):
            raise HTTPException(
                status_code=400,
                detail="Only SQL files are allowed"
            )
        
        # Create kb_document directory if it doesn't exist
        kb_dir = "kb_document"
        os.makedirs(kb_dir, exist_ok=True)
        
        # Save the uploaded file
        file_path = os.path.join(kb_dir, file.filename)
        file_content = await file.read()
        
        # Write the file (overwrite if exists)
        with open(file_path, "wb") as buffer:
            buffer.write(file_content)
        
        # Get username from token or form
        username = uploader
        if not username and token_data:
            # Try to get user ID from token and convert to username
            user_id = token_data.get("user_id")
            if user_id:
                try:
                    user_profile = await user_service.get_user_profile(user_id)
                    username = user_profile.get("username", str(user_id))
                except:
                    username = str(user_id)
        
        # Insert record into opensearch_doc_status table
        # First check if the table exists, create if not
        await ensure_opensearch_doc_status_table()
        
        # Record status in PostgreSQL
        document_type = "SQL"
        insert_query = """
        INSERT INTO opensearch_doc_status 
        (document_name, document_type, process_status, uploader, index_name)
        VALUES (:document_name, :document_type, :process_status, :uploader, :index_name)
        RETURNING id
        """
        
        result = await postgres_service.execute_query(
            insert_query, 
            {
                "document_name": file.filename,
                "document_type": document_type,
                "process_status": "pending",
                "uploader": username,
                "index_name": process_index
            }
        )
        
        # Get the document ID
        doc_id = result[0]['id'] if result and len(result) > 0 else None
        
        # Process the file directly (similar to import_procedure_embedding.py)
        asyncio.create_task(process_procedure_file_background(file_path, doc_id, process_index, file.filename))
        
        return {"status": "success", "message": "file upload success.start process kb document"}
        
    except Exception as e:
        logger.error(f"Error uploading SQL file for OpenSearch: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error uploading SQL file: {str(e)}"
        )

async def process_procedure_file_background(file_path, doc_id, index_name, file_name):
    """
    Background task to process SQL procedure file for OpenSearch
    following the same approach as import_procedure_embedding.py
    
    Args:
        file_path: Path to the SQL file
        doc_id: Document ID in opensearch_doc_status table
        index_name: Name of the OpenSearch index to use
        file_name: Original file name
    """
    try:
        logger.info(f"Processing SQL procedure file for OpenSearch: {file_path} to index: {index_name}")
        
        # Update status to "processing"
        if doc_id:
            update_query = """
            UPDATE opensearch_doc_status 
            SET process_status = 'processing' 
            WHERE id = :id
            """
            await postgres_service.execute_query(update_query, {"id": doc_id})
        
        # We'll use the opensearch_tools directly which has the proven working implementation
        opensearch_tools = OpenSearchTools()
        
        # Process the SQL file using the opensearch_tools implementation
        success, message = await opensearch_tools.process_sql_file(
            file_path,
            embedding_service,
            index_name
        )
        
        # Update status based on success
        if doc_id:
            status = "completed" if success else "failed"
            error_message = "" if success else message
            
            update_query = """
            UPDATE opensearch_doc_status 
            SET process_status = :status,
                error_message = :error_message
            WHERE id = :id
            """
            
            await postgres_service.execute_query(
                update_query,
                {
                    "id": doc_id, 
                    "status": status, 
                    "error_message": error_message[:500] if error_message else None
                }
            )
        
        logger.info(f"SQL file processing result: {success}, {message}")
            
    except Exception as e:
        logger.error(f"Error processing SQL procedure file {file_path}: {str(e)}")
        # Update status to "failed" if there's an error
        if doc_id:
            try:
                update_query = """
                UPDATE opensearch_doc_status 
                SET process_status = 'failed',
                    error_message = :error_message
                WHERE id = :id
                """
                
                await postgres_service.execute_query(
                    update_query,
                    {"id": doc_id, "error_message": str(e)[:500]}
                )
            except Exception as update_error:
                logger.error(f"Error updating status: {str(update_error)}")

async def ensure_opensearch_doc_status_table():
    """Ensure the opensearch_doc_status table exists in the database"""
    try:
        # Check if the table exists
        check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'opensearch_doc_status'
        ) as exists
        """
        
        result = await postgres_service.execute_query(check_query)
        table_exists = result[0]['exists'] if result else False
        
        if not table_exists:
            logger.info("Creating opensearch_doc_status table")
            
            # Create the table with index_name column and error_message
            create_query = """
            CREATE TABLE opensearch_doc_status (
                id SERIAL PRIMARY KEY,
                document_name VARCHAR(255) NOT NULL,
                document_type VARCHAR(50),
                process_status VARCHAR(20) DEFAULT 'pending',
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                uploader VARCHAR(100),
                index_name VARCHAR(100),
                error_message TEXT
            )
            """
            
            await postgres_service.execute_query(create_query)
            logger.info("opensearch_doc_status table created successfully")
        else:
            # Check if columns exist and add them if not
            
            # Check if index_name column exists
            column_check_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = 'opensearch_doc_status' AND column_name = 'index_name'
            ) as exists
            """
            
            column_result = await postgres_service.execute_query(column_check_query)
            column_exists = column_result[0]['exists'] if column_result else False
            
            if not column_exists:
                # Add index_name column if it doesn't exist
                alter_query = """
                ALTER TABLE opensearch_doc_status
                ADD COLUMN index_name VARCHAR(100)
                """
                
                await postgres_service.execute_query(alter_query)
                logger.info("Added index_name column to opensearch_doc_status table")
                
            # Check if error_message column exists
            error_column_check_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = 'opensearch_doc_status' AND column_name = 'error_message'
            ) as exists
            """
            
            error_column_result = await postgres_service.execute_query(error_column_check_query)
            error_column_exists = error_column_result[0]['exists'] if error_column_result else False
            
            if not error_column_exists:
                # Add error_message column if it doesn't exist
                alter_query = """
                ALTER TABLE opensearch_doc_status
                ADD COLUMN error_message TEXT
                """
                
                await postgres_service.execute_query(alter_query)
                logger.info("Added error_message column to opensearch_doc_status table")
    
    except Exception as e:
        logger.error(f"Error ensuring opensearch_doc_status table exists: {str(e)}")
        raise

@app.get("/kb/opensearch/status")
async def get_opensearch_document_status(token_data: dict = Depends(verify_token)):
    """
    Get status of all OpenSearch document processing records
    
    Returns:
        List of document status records
    """
    try:
        result = await opensearch_procedure_service.get_document_status()
        return result
        
    except Exception as e:
        logger.error(f"Error getting OpenSearch document status: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting document status: {str(e)}"
        )

@app.post("/kb/opensearch/delete")
async def delete_opensearch_document(request: Request, token_data: dict = Depends(verify_token)):
    """
    Delete document from OpenSearch and related record from opensearch_doc_status
    
    Request body format:
        {
            "id": "8", 
            "document_name": "procedure_mock.sql"
        }
        
    Returns:
        Response with status and message
    """
    try:
        # Parse JSON request body
        data = await request.json()
        document_name = data.get("document_name")
        doc_id = data.get("id")
        
        if not document_name:
            raise HTTPException(
                status_code=400,
                detail="Missing required parameter: document_name"
            )
            
        if not doc_id:
            raise HTTPException(
                status_code=400,
                detail="Missing required parameter: id"
            )
        
        # Get the document information from the database
        select_query = """
        SELECT id, document_name, index_name 
        FROM opensearch_doc_status 
        WHERE id = :id
        """
        
        result = await postgres_service.execute_query(
            select_query,
            {"id": doc_id}
        )
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Document with ID {doc_id} not found"
            )
        
        # Get the index name from the record
        index_name = result[0].get("index_name", "procedure_index")
        
        # Try to delete from OpenSearch
        # This is a best-effort operation - we'll continue even if this fails
        try:
            # Create OpenSearch tools
            opensearch_tools = OpenSearchTools()
            delete_result = opensearch_tools.delete_document_by_name(index_name, document_name)
            
            if delete_result["status"] != "success":
                logger.warning(f"Warning when deleting from OpenSearch: {delete_result['message']}")
            else:
                logger.info(f"Successfully deleted from OpenSearch: {delete_result['message']}")
        except Exception as e:
            logger.warning(f"Unable to delete from OpenSearch: {str(e)}")
            # Continue processing - we'll still delete the database record
        
        # Delete record from opensearch_doc_status
        delete_status_query = """
        DELETE FROM opensearch_doc_status
        WHERE id = :id
        """
        
        await postgres_service.execute_query(
            delete_status_query,
            {"id": doc_id}
        )
        
        logger.info(f"Deleted document status record with ID {doc_id}")
        
        return {
            "status": "success", 
            "message": "Document deleted successfully"
        }
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error deleting document: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting document: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    
# uvicorn server:app --host 0.0.0.0 --port 8000