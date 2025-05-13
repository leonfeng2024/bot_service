import asyncio
import os
import logging
from tools.opensearch_tools import OpenSearchTools
from service.embedding_service import EmbeddingService
from service.postgres_service import PostgresService

logger = logging.getLogger(__name__)

class OpenSearchProcedureService:
    """Service for handling SQL procedure processing and indexing to OpenSearch"""
    
    def __init__(self):
        self.opensearch_tools = OpenSearchTools()
        self.embedding_service = EmbeddingService()
        self.postgres_service = PostgresService()
        
    async def process_sql_file(self, file_path: str, doc_id: int = None, index_name: str = "procedure_index") -> bool:
        """
        Process SQL file and index to OpenSearch
        
        Args:
            file_path: Path to SQL file
            doc_id: Document ID in opensearch_doc_status table (optional)
            index_name: Name of the index to use (default: procedure_index)
            
        Returns:
            True if processing was successful, False otherwise
        """
        try:
            logger.info(f"Processing SQL file for OpenSearch: {file_path} to index: {index_name}")
            
            # Process and index the file
            success, message = await self.opensearch_tools.process_sql_file(
                file_path, 
                self.embedding_service,
                index_name
            )
            
            # Update document status if doc_id is provided
            if doc_id:
                status = "completed" if success else "failed"
                await self.update_document_status(doc_id, status, message, index_name)
                
            return success
        except Exception as e:
            logger.error(f"Error processing SQL file for OpenSearch: {str(e)}")
            
            # Update document status if doc_id is provided
            if doc_id:
                await self.update_document_status(doc_id, "failed", str(e))
                
            return False
    
    async def update_document_status(self, doc_id: int, status: str, message: str = None, index_name: str = None) -> None:
        """
        Update document status in opensearch_doc_status table
        
        Args:
            doc_id: Document ID
            status: New status (completed, failed, etc.)
            message: Optional status message
            index_name: Name of the index (optional)
        """
        try:
            # Include index name in the update if provided
            if index_name:
                query = """
                UPDATE opensearch_doc_status 
                SET process_status = :status, index_name = :index_name
                WHERE id = :id
                """
                
                await self.postgres_service.execute_query(
                    query,
                    {"id": doc_id, "status": status, "index_name": index_name}
                )
            else:
                query = """
                UPDATE opensearch_doc_status 
                SET process_status = :status
                WHERE id = :id
                """
                
                await self.postgres_service.execute_query(
                    query,
                    {"id": doc_id, "status": status}
                )
            
            logger.info(f"Updated document status for ID {doc_id} to {status}")
        except Exception as e:
            logger.error(f"Error updating document status: {str(e)}")
    
    async def get_document_status(self) -> list:
        """
        Get list of all OpenSearch document status records
        
        Returns:
            List of status records
        """
        try:
            query = """
            SELECT 
                id,
                document_name,
                document_type,
                process_status,
                upload_date,
                uploader,
                index_name,
                error_message
            FROM 
                opensearch_doc_status
            ORDER BY 
                upload_date DESC
            """
            
            result = await self.postgres_service.execute_query(query)
            return result
        except Exception as e:
            logger.error(f"Error getting document status: {str(e)}")
            return [] 