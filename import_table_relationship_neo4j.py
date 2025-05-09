#!/usr/bin/env python3
import asyncio
import sys
import os
import json
from datetime import datetime

# Add project root directory to Python path to ensure service module can be imported
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from service.neo4j_service import Neo4jService

# Helper function to handle Neo4j result JSON serialization
def make_serializable(obj):
    """Convert Neo4j result object to JSON serializable dictionary"""
    if isinstance(obj, dict):
        return {key: make_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [make_serializable(item) for item in obj]
    elif hasattr(obj, '__dict__'):
        # For objects with __dict__ attribute, try to convert to dictionary
        try:
            return {key: make_serializable(value) for key, value in obj.__dict__.items() 
                    if not key.startswith('_')}
        except:
            return str(obj)
    else:
        # Convert to string for non-serializable objects
        try:
            json.dumps(obj)
            return obj
        except:
            return str(obj)

async def clear_all_data():
    """Clear all nodes and relationships from Neo4j database"""
    print(f"[{datetime.now()}] Starting to clear all data from database...")
    
    neo4j_service = Neo4jService()
    try:
        # Execute Cypher query to clear all data
        query = "MATCH (n) DETACH DELETE n"
        neo4j_service.neo4j.execute_query(query)
        print(f"[{datetime.now()}] ✅ Successfully cleared all data")
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Error occurred while clearing data: {str(e)}")
        import traceback
        print(traceback.format_exc())
    finally:
        neo4j_service.close()

# Sample table relationships - these relationships will be imported into Neo4j database
SAMPLE_RELATIONSHIPS = {
    # Employee and Department relationships
    "employees.employee_id": "departments.department_id",
    "employees.manager_id": "employees.employee_id",
    "departments.manager_id": "employees.employee_id",
    
    # Job History
    "job_history.employee_id": "employees.employee_id",
    "job_history.department_id": "departments.department_id",
    "job_history.job_id": "jobs.job_id",
    
    # Geographic Location
    "countries.region_id": "regions.region_id",
    "locations.country_id": "countries.country_id",
    "departments.location_id": "locations.location_id",
    
    # View relationships
    "v_employee_details.employee_id": "employees.employee_id", 
    "v_employee_details.department_id": "departments.department_id", 
    "v_department_summary.department_id": "departments.department_id",
    "v_salary_report.job_id": "jobs.job_id",
    
    # Stored Procedure relationships
    "p_update_employee.employee_id": "employees.employee_id",
    "p_transfer_employee.employee_id": "employees.employee_id",
    "p_transfer_employee.department_id": "departments.department_id",
    "p_calculate_bonus.employee_id": "employees.employee_id",
    "p_get_department_staff.department_id": "departments.department_id"
}

async def import_table_relationships():
    """
    Import table relationships into Neo4j database
    """
    print(f"[{datetime.now()}] Starting to import table relationships into Neo4j database...")
    
    # Initialize Neo4j service
    neo4j_service = Neo4jService()
    
    try:
        # 1. Check Neo4j connection
        if not neo4j_service.neo4j.connected:
            print("Attempting to reconnect to Neo4j...")
            neo4j_service.neo4j._connect()
            if not neo4j_service.neo4j.connected:
                print("Unable to connect to Neo4j database, please check configuration")
                return False
        
        # 2. Clean existing data - Use with caution, this will delete all tables and relationships
        print("Cleaning existing data...")
        neo4j_service.neo4j.execute_query("MATCH (n) DETACH DELETE n")
        
        # 3. Import table and view nodes
        print("Importing table and view nodes...")
        
        # Collect all table and view names
        all_tables = set()
        for relation in SAMPLE_RELATIONSHIPS.items():
            source, target = relation
            source_table = source.split('.')[0]
            target_table = target.split('.')[0]
            all_tables.add(source_table)
            all_tables.add(target_table)
        
        # Create all table and view nodes
        for table in all_tables:
            # Determine node label - views start with v_
            node_label = "View" if table.startswith('v_') else "Table"
            
            # Create node
            query = f"""
            MERGE (t:{node_label} {{name: $table_name}})
            RETURN t
            """
            neo4j_service.neo4j.execute_query(query, parameters={"table_name": table})
            print(f"Created {node_label} node: {table}")
        
        # 4. Import relationships
        print(f"Importing {len(SAMPLE_RELATIONSHIPS)} table relationships...")
        
        success = await neo4j_service.import_table_relationships(SAMPLE_RELATIONSHIPS)
        
        if success:
            print("Table relationships imported successfully")
            
            # 5. Verify imported data
            print("Verifying imported data...")
            node_count = neo4j_service.neo4j.get_node_count()
            relationship_count = neo4j_service.neo4j.get_relationship_count()
            
            print(f"Node count: {node_count}")
            print(f"Relationship count: {relationship_count}")
            
            # 6. Query sample relationships
            print("Querying sample relationships...")
            query = """
            MATCH (a)-[r:RELATED_TO]->(b)
            RETURN a.name as source_table, b.name as target_table, 
                   r.source_field as source_field, r.target_field as target_field
            LIMIT 10
            """
            
            results = neo4j_service.neo4j.execute_query(query)
            for result in results:
                print(f"{result.get('source_table')}.{result.get('source_field')} -> {result.get('target_table')}.{result.get('target_field')}")
                
            return True
        else:
            print("Failed to import table relationships")
            return False
            
    except Exception as e:
        import traceback
        print(f"Error importing table relationships: {str(e)}")
        print(traceback.format_exc())
        return False
    finally:
        try:
            # Close Neo4j connection
            neo4j_service.close()
        except:
            pass

def get_visualization_query():
    """Get Cypher query for visualizing graph in Neo4j browser"""
    return """
    MATCH (a:Table)-[r:RELATED_TO]->(b:Table)
    RETURN a, r, b
    LIMIT 100
    """

async def main():
    # 1. Clear all data
    await clear_all_data()
    
    # 2. Import new relationship data
    await import_table_relationships()
    
    # 3. Output visualization query
    print("\n=== Neo4j Browser Visualization Query ===")
    print("Execute the following query in Neo4j browser to view the relationship graph:")
    print(get_visualization_query())
    print("\nTips:")
    print("1. Execute the above query in Neo4j browser")
    print("2. Click 'Graph' view in the results panel")
    print("3. Drag nodes to adjust layout")
    print("4. Click nodes to expand/collapse details")

if __name__ == "__main__":
    # Run main function
    asyncio.run(main())
