import asyncio
import pandas as pd
from service.export_ppt_service import ExportPPTService

async def main():
    # Create test data similar to Neo4j format
    test_data = [
        {
            'source_table': 'p_calculate_bonus', 
            'target_table': 'employees', 
            'source_field': 'employee_id', 
            'target_field': 'employee_id'
        },
        {
            'source_table': 'p_transfer_employee', 
            'target_table': 'departments', 
            'source_field': 'department_id', 
            'target_field': 'department_id'
        },
        {
            'source_table': 'p_transfer_employee', 
            'target_table': 'employees', 
            'source_field': 'employee_id', 
            'target_field': 'employee_id'
        },
        {
            'source_table': 'employees', 
            'target_table': 'departments', 
            'source_field': 'department_id', 
            'target_field': 'department_id'
        }
    ]
    
    # Create DataFrame
    df = pd.DataFrame(test_data)
    print(f"DataFrame columns: {df.columns.tolist()}")
    
    # Create service instance
    service = ExportPPTService()
    
    # Prepare data
    prepared_df = service._prepare_neo4j_data(df)
    print(f"Prepared DataFrame columns: {prepared_df.columns.tolist()}")
    
    # Generate diagram
    print("Generating diagram...")
    result = await service.create_mermaid_diagram(prepared_df)
    
    print(f"Diagram generation result: {result}")

if __name__ == "__main__":
    asyncio.run(main()) 