from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from service.chat_service import ChatService
from service.llm_service import LLMService
from service.embedding_service import EmbeddingService
from service.neo4j_service import Neo4jService
from service.export_excel_service import ExportExcelService
from service.export_ppt_service import ExportPPTService
from models.models import ChatRequest, DatabaseSchemaRequest
import logging
import os

app = FastAPI()

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

logger = logging.getLogger(__name__)

@app.on_event("startup")
async def startup_event():
    pass

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on server shutdown."""
    neo4j_service.close()
    await export_service.close()

@app.post("/chat")
async def chat(request: ChatRequest):
    return await chat_service.handle_chat(request.username, request.query)

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
