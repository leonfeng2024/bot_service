from opensearchpy import OpenSearch
from typing import Dict, Any, List
import traceback

class OpenSearchConfig:
    """Configuration for OpenSearch connection."""
    HOST = "bibot_opensearch"
    PORT = 9200
    URL = f"http://{HOST}:{PORT}"
    AUTH = None
    HEADERS = {"Content-Type": "application/json"}

class OpenSearchTools:
    def __init__(self, config: OpenSearchConfig = OpenSearchConfig):
        """
        Initialize OpenSearch client with configuration.
        
        Args:
            config: OpenSearch configuration class (default: OpenSearchConfig)
        """
        self.config = config
        self.client = OpenSearch(
            hosts=[{"host": config.HOST, "port": config.PORT}],
            http_auth=config.AUTH,
            headers=config.HEADERS
        )

    def get_index_list(self) -> List[str]:
        """
        Get a list of all indices from OpenSearch, excluding system indices.
        
        Returns:
            List[str]: List of index names
        """
        try:
            # 获取所有索引
            response = self.client.indices.get_alias(index="*")
            
            # 过滤掉以 "." 开头的系统索引
            index_list = [
                index_name 
                for index_name in response.keys() 
                if not index_name.startswith(".")
            ]
            
            # 按字母顺序排序
            index_list.sort()
            
            print(f"Found {len(index_list)} indices")
            return index_list
                
        except Exception as e:
            print(f"Error getting index list: {str(e)}")
            print(traceback.format_exc())
            return []
            
    def get_index_infor(self) -> List[Dict[str, str]]:
        """
        Get detailed information about all indices in OpenSearch.
        
        Returns:
            List[Dict[str, str]]: List of index information including name, doc count, deleted docs, and size
        """
        try:
            # Get all indices stats
            stats = self.client.indices.stats(index="_all")
            
            # Get list of non-system indices
            indices = self.get_index_list()
            
            result = []
            for index_name in indices:
                if index_name in stats["indices"]:
                    index_stats = stats["indices"][index_name]["total"]
                    result.append({
                        "index_name": index_name,
                        "doc.count": str(index_stats["docs"]["count"]),
                        "docs.deleted": str(index_stats["docs"]["deleted"]),
                        "store.size": str(index_stats["store"]["size_in_bytes"] / 1024) + "kb"
                    })
            
            return result
                
        except Exception as e:
            print(f"Error getting index information: {str(e)}")
            print(traceback.format_exc())
            return []

    def delete_index_by_name(self, index_name: str) -> Dict[str, str]:
        """
        Delete an OpenSearch index by name.
        
        Args:
            index_name (str): Name of the index to delete
        
        Returns:
            Dict[str, str]: Operation result with status and message
        """
        try:
            # Check if index exists
            if not self.client.indices.exists(index=index_name):
                return {
                    "status": "failed",
                    "message": f"Index {index_name} does not exist"
                }
            
            # Delete the index
            response = self.client.indices.delete(
                index=index_name,
                headers={"Content-Type": "application/json"}
            )
            
            if response.get("acknowledged", False):
                return {
                    "status": "success",
                    "message": "Delete index success"
                }
            else:
                return {
                    "status": "failed",
                    "message": "Delete index failed"
                }
                
        except Exception as e:
            print(f"Error deleting index: {str(e)}")
            print(traceback.format_exc())
            return {
                "status": "failed",
                "message": f"Delete index failed: {str(e)}"
            }

    def bulk_insert_documents(self, index_name: str, document_name: str, documents: List[Dict[str, Any]]) -> Dict[str, str]:
        print(f"bulk_insert_documents: {index_name} {document_name} {documents}")
        try:
            if not self.client.indices.exists(index=index_name):
                return {
                    "status": "failed",
                    "message": f"Index {index_name} does not exist"
                }

            # 先删除现有文档
            self.delete_document_by_name(index_name, document_name)
            
            batch_size = 20
            # Insert documents in batches of 20
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]
                bulk_body = []
                for doc in batch:
                    bulk_body.append({"index": {"_index": index_name}})
                    bulk_body.append(doc)
                
                response = self.client.bulk(
                    body=bulk_body,
                    refresh=True
                )
                
                if response.get("errors", True):
                    return {
                        "status": "failed",
                        "message": "Error during bulk insert"
                    }
            
            return {
                "status": "success",
                "message": f"Successfully inserted {len(documents)} documents"
            }
            
        except Exception as e:
            print(f"Error in bulk insert: {str(e)}")
            print(traceback.format_exc())
            return {
                "status": "failed",
                "message": f"Bulk insert failed: {str(e)}"
            }

    def delete_document_by_name(self, index_name: str, document_name: str) -> Dict[str, str]:
        """
        Delete documents by document name from specified OpenSearch index.
        
        Args:
            index_name (str): Name of the index
            document_name (str): Name of the document to delete
        
        Returns:
            Dict[str, str]: Operation result with status and message
        """
        try:
            # 检查索引是否存在
            if not self.client.indices.exists(index=index_name):
                return {
                    "status": "failed",
                    "message": f"Index {index_name} does not exist"
                }
            
            # 构建删除查询
            query = {
                "query": {
                    "match": {
                        "document_name": document_name
                    }
                }
            }
            
            # 执行删除操作
            response = self.client.delete_by_query(
                index=index_name,
                body=query,
                refresh=True
            )
            
            deleted_count = response.get("deleted", 0)
            if deleted_count > 0:
                return {
                    "status": "success",
                    "message": f"Successfully deleted {deleted_count} documents"
                }
            else:
                return {
                    "status": "failed",
                    "message": "No documents found to delete"
                }
                
        except Exception as e:
            print(f"Error deleting document by name: {str(e)}")
            print(traceback.format_exc())
            return {
                "status": "failed",
                "message": f"Delete failed: {str(e)}"
            }


if __name__ == "__main__":
    # Initialize OpenSearch client with default config
    os_tools = OpenSearchTools()
    
    # Test get_index_list method
    print("Testing get_index_list method...")
    indices = os_tools.get_index_list()
    print("\nAvailable indices:")
    for index in indices:
        print(f"- {index}")
