import pandas as pd
from service.neo4j_service import Neo4jService
from datetime import datetime
import os

class ExportExcelService:
    def __init__(self):
        self.neo4j_service = Neo4jService()

    async def export_relationships_to_excel(self, output_file: str = "relationship.xlsx"):
        """
        Export table relationships from Neo4j to Excel file.
        
        Args:
            output_file (str): Path to the output Excel file
        """
        try:
            # Query to get all relationships
            query = """
            MATCH (source)-[r]->(target)
            RETURN 
                labels(source) as source_labels,
                properties(source) as source_properties,
                type(r) as relationship_type,
                properties(r) as relationship_properties,
                labels(target) as target_labels,
                properties(target) as target_properties
            """
            
            result = self.neo4j_service.neo4j.execute_query(query)
            
            # Prepare data for Excel
            relationships_data = []
            
            for record in result:
                source_label = record["source_labels"][0] if record["source_labels"] else ""
                target_label = record["target_labels"][0] if record["target_labels"] else ""
                
                source_props = record["source_properties"]
                target_props = record["target_properties"]
                
                relationship = {
                    "Source Table": source_label,
                    "Source ID Field": "company_id" if source_label == "m_distributor" else "",
                    "Source ID Value": source_props.get("company_id", ""),
                    "Source Name": source_props.get("distributor_name", "") or source_props.get("company_name", ""),
                    "Relationship Type": record["relationship_type"],
                    "Target Table": target_label,
                    "Target ID Field": "company_id" if target_label == "m_company" else "",
                    "Target ID Value": target_props.get("company_id", ""),
                    "Target Name": target_props.get("company_name", "") or target_props.get("distributor_name", ""),
                    "Created At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                relationships_data.append(relationship)
            
            # Create DataFrame
            df = pd.DataFrame(relationships_data)
            
            # Create Excel file
            if os.path.exists(output_file):
                # If file exists, read existing data and append new data
                existing_df = pd.read_excel(output_file)
                df = pd.concat([existing_df, df], ignore_index=True)
            
            # Write to Excel
            df.to_excel(output_file, index=False, engine='openpyxl')
            
            return True
            
        except Exception as e:
            print(f"Error exporting relationships to Excel: {str(e)}")
            return False
        
    async def close(self):
        """Close Neo4j connection"""
        self.neo4j_service.close() 