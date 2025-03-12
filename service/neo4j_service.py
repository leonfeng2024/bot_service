from typing import Dict, Any, List, Optional
from tools.neo4j_tools import Neo4jTools
from utils.singleton import singleton
import json
from datetime import datetime


@singleton
class Neo4jService:
    def __init__(self):
        """Initialize Neo4j service with tools."""
        self.neo4j = Neo4jTools()

    async def create_node(
        self,
        label: str,
        properties: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Create a new node in Neo4j.

        Args:
            label: Node label
            properties: Node properties

        Returns:
            Created node data if successful, None otherwise
        """
        return self.neo4j.create_node(label, properties)

    async def get_nodes(
        self,
        label: str,
        conditions: Optional[Dict[str, Any]] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get nodes matching the specified criteria.

        Args:
            label: Node label to search for
            conditions: Optional dictionary of property conditions
            limit: Maximum number of nodes to return

        Returns:
            List of matching nodes
        """
        try:
            # Build the WHERE clause if conditions are provided
            where_clause = ""
            if conditions:
                conditions_list = []
                for key, value in conditions.items():
                    if isinstance(value, str):
                        conditions_list.append(f"n.{key} = '{value}'")
                    else:
                        conditions_list.append(f"n.{key} = {value}")
                if conditions_list:
                    where_clause = "WHERE " + " AND ".join(conditions_list)

            # Construct and execute query
            query = f"""
            MATCH (n:{label})
            {where_clause}
            RETURN n
            LIMIT {limit}
            """
            results = self.neo4j.execute_query(query)
            return [dict(record["n"]) for record in results]

        except Exception as e:
            import traceback
            print(f"Error getting nodes: {str(e)}")
            print(traceback.format_exc())
            return []

    async def update_node(
        self,
        label: str,
        match_properties: Dict[str, Any],
        update_properties: Dict[str, Any]
    ) -> bool:
        """
        Update nodes matching the specified criteria.

        Args:
            label: Node label to update
            match_properties: Properties to identify the nodes to update
            update_properties: New property values to set

        Returns:
            True if successful, False otherwise
        """
        try:
            # Build the match conditions
            match_conditions = []
            for key, value in match_properties.items():
                if isinstance(value, str):
                    match_conditions.append(f"n.{key} = '{value}'")
                else:
                    match_conditions.append(f"n.{key} = {value}")

            # Build the SET clause
            set_items = []
            for key, value in update_properties.items():
                if isinstance(value, str):
                    set_items.append(f"n.{key} = '{value}'")
                else:
                    set_items.append(f"n.{key} = {value}")

            # Construct and execute query
            query = f"""
            MATCH (n:{label})
            WHERE {" AND ".join(match_conditions)}
            SET {", ".join(set_items)}
            RETURN n
            """
            results = self.neo4j.execute_query(query)
            return len(results) > 0

        except Exception as e:
            import traceback
            print(f"Error updating nodes: {str(e)}")
            print(traceback.format_exc())
            return False

    async def delete_node(
        self,
        label: str,
        conditions: Dict[str, Any],
        delete_relationships: bool = True
    ) -> bool:
        """
        Delete nodes matching the specified criteria.

        Args:
            label: Node label to delete
            conditions: Properties to identify the nodes to delete
            delete_relationships: Whether to delete relationships as well

        Returns:
            True if successful, False otherwise
        """
        try:
            # Build the WHERE clause
            where_conditions = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    where_conditions.append(f"n.{key} = '{value}'")
                else:
                    where_conditions.append(f"n.{key} = {value}")

            # Construct and execute query
            if delete_relationships:
                query = f"""
                MATCH (n:{label})
                WHERE {" AND ".join(where_conditions)}
                DETACH DELETE n
                """
            else:
                query = f"""
                MATCH (n:{label})
                WHERE {" AND ".join(where_conditions)}
                DELETE n
                """
            
            self.neo4j.execute_query(query)
            return True

        except Exception as e:
            import traceback
            print(f"Error deleting nodes: {str(e)}")
            print(traceback.format_exc())
            return False

    async def create_relationship(
        self,
        start_label: str,
        start_properties: Dict[str, Any],
        end_label: str,
        end_properties: Dict[str, Any],
        relationship_type: str,
        relationship_properties: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Create a relationship between two nodes.

        Args:
            start_label: Label of the start node
            start_properties: Properties to identify the start node
            end_label: Label of the end node
            end_properties: Properties to identify the end node
            relationship_type: Type of relationship to create
            relationship_properties: Optional properties for the relationship

        Returns:
            True if successful, False otherwise
        """
        return self.neo4j.create_relationship(
            start_label,
            start_properties,
            end_label,
            end_properties,
            relationship_type,
            relationship_properties
        )

    async def get_relationships(
        self,
        start_label: Optional[str] = None,
        end_label: Optional[str] = None,
        relationship_type: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get relationships matching the specified criteria.

        Args:
            start_label: Optional label of the start node
            end_label: Optional label of the end node
            relationship_type: Optional type of relationship
            limit: Maximum number of relationships to return

        Returns:
            List of matching relationships with their connected nodes
        """
        try:
            # Build the match pattern
            start_pattern = f":{start_label}" if start_label else ""
            end_pattern = f":{end_label}" if end_label else ""
            rel_pattern = f":{relationship_type}" if relationship_type else ""

            # Construct and execute query
            query = f"""
            MATCH (a{start_pattern})-[r{rel_pattern}]->(b{end_pattern})
            RETURN a, r, b
            LIMIT {limit}
            """
            results = self.neo4j.execute_query(query)
            
            # Format results
            formatted_results = []
            for record in results:
                formatted_results.append({
                    "start_node": dict(record["a"]),
                    "relationship": dict(record["r"]),
                    "end_node": dict(record["b"])
                })
            return formatted_results

        except Exception as e:
            import traceback
            print(f"Error getting relationships: {str(e)}")
            print(traceback.format_exc())
            return []

    async def delete_relationship(
        self,
        start_label: str,
        start_properties: Dict[str, Any],
        end_label: str,
        end_properties: Dict[str, Any],
        relationship_type: Optional[str] = None
    ) -> bool:
        """
        Delete relationships between specified nodes.

        Args:
            start_label: Label of the start node
            start_properties: Properties to identify the start node
            end_label: Label of the end node
            end_properties: Properties to identify the end node
            relationship_type: Optional type of relationship to delete

        Returns:
            True if successful, False otherwise
        """
        try:
            # Build the match conditions
            start_conditions = []
            for key, value in start_properties.items():
                if isinstance(value, str):
                    start_conditions.append(f"a.{key} = '{value}'")
                else:
                    start_conditions.append(f"a.{key} = {value}")

            end_conditions = []
            for key, value in end_properties.items():
                if isinstance(value, str):
                    end_conditions.append(f"b.{key} = '{value}'")
                else:
                    end_conditions.append(f"b.{key} = {value}")

            # Build the relationship pattern
            rel_pattern = f":{relationship_type}" if relationship_type else ""

            # Construct and execute query
            query = f"""
            MATCH (a:{start_label})-[r{rel_pattern}]->(b:{end_label})
            WHERE {" AND ".join(start_conditions)} AND {" AND ".join(end_conditions)}
            DELETE r
            """
            self.neo4j.execute_query(query)
            return True

        except Exception as e:
            import traceback
            print(f"Error deleting relationships: {str(e)}")
            print(traceback.format_exc())
            return False

    def close(self) -> None:
        """Close the Neo4j connection."""
        self.neo4j.close()

    async def import_table_schema(self, schema_json: str) -> bool:
        """
        Import a table schema from JSON and create corresponding nodes in Neo4j.

        Args:
            schema_json: JSON string containing table schema information

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Parse JSON
            schema = json.loads(schema_json)
            table_name = schema.get("table_name")
            
            # First, delete any existing nodes and relationships
            cleanup_query = f"""
            MATCH (n:{table_name})
            DETACH DELETE n
            """
            self.neo4j.execute_query(cleanup_query)

            # Create nodes with data
            if table_name == "m_company":
                create_query = """
                UNWIND $data AS company
                CREATE (c:m_company {
                    company_id: company.company_id,
                    company_name: company.company_name,
                    address: company.address,
                    phone: company.phone,
                    created_at: datetime()
                })
                """
                self.neo4j.execute_query(create_query, parameters={"data": schema.get("data", [])})

            elif table_name == "m_distributor":
                create_query = """
                UNWIND $data AS distributor
                CREATE (d:m_distributor {
                    distributor_id: distributor.distributor_id,
                    distributor_name: distributor.distributor_name,
                    contact_info: distributor.contact_info,
                    company_id: distributor.company_id,
                    created_at: datetime()
                })
                """
                self.neo4j.execute_query(create_query, parameters={"data": schema.get("data", [])})

            return True

        except Exception as e:
            import traceback
            print(f"Error importing table schema: {str(e)}")
            print(traceback.format_exc())
            return False

    async def create_foreign_key_relationships(self) -> bool:
        """
        Create relationships between m_distributor and m_company nodes.

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create relationships between distributors and companies
            relationship_query = """
            MATCH (d:m_distributor), (c:m_company)
            WHERE d.company_id = c.company_id
            CREATE (d)-[:BELONGS_TO]->(c)
            """
            self.neo4j.execute_query(relationship_query)
            return True

        except Exception as e:
            import traceback
            print(f"Error creating relationships: {str(e)}")
            print(traceback.format_exc())
            return False

    async def import_database_schema(self, schema_jsons: List[str]) -> bool:
        """
        Import multiple table schemas and create their relationships.

        Args:
            schema_jsons: List of JSON strings containing table schemas

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # First, import all table schemas
            for schema_json in schema_jsons:
                success = await self.import_table_schema(schema_json)
                if not success:
                    return False

            # Then create foreign key relationships
            return await self.create_foreign_key_relationships()

        except Exception as e:
            import traceback
            print(f"Error importing database schema: {str(e)}")
            print(traceback.format_exc())
            return False

    async def import_table_relationships(self, relationships_json: str, relationship_type: str = "RELATED_TO") -> bool:
        """
        Import table relationships from JSON and create corresponding relationships in Neo4j.
        
        The JSON should be in the format:
        {
            "source_table.source_field": "target_table.target_field",
            ...
        }
        
        Args:
            relationships_json: JSON string containing table relationship information
            relationship_type: Type of relationship to create (default: "RELATED_TO")
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Parse JSON
            if isinstance(relationships_json, str):
                relationships = json.loads(relationships_json)
            else:
                relationships = relationships_json
                
            success_count = 0
            total_count = len(relationships)
            
            # Process each relationship
            for source, target in relationships.items():
                try:
                    # Parse source and target
                    source_parts = source.split(".")
                    target_parts = target.split(".")
                    
                    if len(source_parts) != 2 or len(target_parts) != 2:
                        print(f"Invalid format for relationship: {source} -> {target}")
                        continue
                    
                    source_table, source_field = source_parts
                    target_table, target_field = target_parts
                    
                    # Create Cypher query for this relationship
                    relationship_properties = {
                        "source_field": source_field,
                        "target_field": target_field,
                        "created_at": datetime.now().isoformat()
                    }
                    
                    query = """
                    MERGE (a:Table {name: $source_table})
                    MERGE (b:Table {name: $target_table})
                    CREATE (a)-[r:RELATED_TO {
                        source_field: $source_field,
                        target_field: $target_field,
                        relationship_description: $relationship_description,
                        created_at: $created_at
                    }]->(b)
                    """
                    
                    # Execute query
                    self.neo4j.execute_query(query, parameters={
                        "source_table": source_table,
                        "target_table": target_table,
                        "source_field": source_field,
                        "target_field": target_field,
                        "relationship_description": f"{source_field} -> {target_field}",
                        "created_at": datetime.now().isoformat()
                    })
                    
                    success_count += 1
                    print(f"Successfully imported relationship: {source} -> {target}")
                    
                except Exception as e:
                    print(f"Error importing relationship {source} -> {target}: {str(e)}")
                    continue
                    
            print(f"Imported {success_count} out of {total_count} relationships")
            return success_count > 0
            
        except Exception as e:
            import traceback
            print(f"Error importing table relationships: {str(e)}")
            print(traceback.format_exc())
            return False
            
    async def import_table_relationship(self, source_table: str, source_field: str, 
                                        target_table: str, target_field: str, 
                                        relationship_type: str = "RELATED_TO") -> bool:
        """
        Import a single table relationship into Neo4j.
        
        Args:
            source_table: Name of the source table
            source_field: Field in the source table
            target_table: Name of the target table
            target_field: Field in the target table
            relationship_type: Type of relationship to create (default: "RELATED_TO")
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create Cypher query for this relationship
            query = """
            MERGE (a:Table {name: $source_table})
            MERGE (b:Table {name: $target_table})
            CREATE (a)-[r:RELATED_TO {
                source_field: $source_field,
                target_field: $target_field,
                relationship_description: $relationship_description,
                created_at: $created_at
            }]->(b)
            """
            
            # Execute query
            self.neo4j.execute_query(query, parameters={
                "source_table": source_table,
                "target_table": target_table,
                "source_field": source_field,
                "target_field": target_field,
                "relationship_description": f"{source_field} -> {target_field}",
                "created_at": datetime.now().isoformat()
            })
            
            print(f"Successfully imported relationship: {source_table}.{source_field} -> {target_table}.{target_field}")
            return True
            
        except Exception as e:
            import traceback
            print(f"Error importing table relationship: {str(e)}")
            print(traceback.format_exc())
            return False

    async def get_v_relationships(self, depth: int = 2, limit: int = 100) -> Dict[str, Any]:
        """
        Get all relationships and nodes connected to views (nodes with names starting with 'v_').
        
        Args:
            depth: Maximum path traversal depth (default: 2)
            limit: Maximum number of results to return (default: 100)
            
        Returns:
            Dict containing nodes and relationships information
        """
        try:
            # Query to find all view nodes and their relationships
            query = f"""
            MATCH path = (n:Table)-[*1..{depth}]-(m:Table)
            WHERE n.name =~ 'v_.*' OR m.name =~ 'v_.*'
            RETURN path
            LIMIT {limit}
            """
            
            results = self.neo4j.execute_query(query)
            
            # Process results
            nodes = {}
            relationships = []
            
            for record in results:
                path = record["path"]
                
                # Extract nodes and relationships from path
                for node in path.nodes:
                    node_id = node.id
                    if node_id not in nodes:
                        node_data = dict(node.items())
                        node_data["id"] = node_id
                        node_data["labels"] = list(node.labels)
                        nodes[node_id] = node_data
                
                for relationship in path.relationships:
                    rel_data = {
                        "id": relationship.id,
                        "type": relationship.type,
                        "properties": dict(relationship.items()),
                        "start_node": relationship.start_node.id,
                        "end_node": relationship.end_node.id
                    }
                    relationships.append(rel_data)
            
            return {
                "nodes": list(nodes.values()),
                "relationships": relationships
            }
            
        except Exception as e:
            import traceback
            print(f"Error getting view relationships: {str(e)}")
            print(traceback.format_exc())
            return {"nodes": [], "relationships": []}

    async def find_v_node_connections(self, view_name: str, direction: str = "both", limit: int = 100) -> Dict[str, Any]:
        """
        Find all nodes and relationships connected to a specific view node.
        
        Args:
            view_name: The name of the view node to find connections for (should start with 'v_')
            direction: Direction of relationships to traverse ('in', 'out', or 'both')
            limit: Maximum number of results to return
            
        Returns:
            Dict containing connected nodes and relationships information
        """
        try:
            # Build direction pattern based on parameter
            direction_pattern = ""
            if direction == "in":
                direction_pattern = "<-[r]-"
            elif direction == "out":
                direction_pattern = "-[r]->"
            else:  # both
                direction_pattern = "-[r]-"
            
            # Query to find connections
            query = f"""
            MATCH (n:Table){direction_pattern}(m:Table)
            WHERE n.name = $view_name
            RETURN n, r, m
            LIMIT {limit}
            """
            
            results = self.neo4j.execute_query(query, parameters={"view_name": view_name})
            
            # Process results
            nodes = {}
            relationships = []
            
            for record in results:
                # Add source node
                source_node = record["n"]
                source_id = source_node.id
                if source_id not in nodes:
                    source_data = dict(source_node.items())
                    source_data["id"] = source_id
                    source_data["labels"] = list(source_node.labels)
                    nodes[source_id] = source_data
                
                # Add target node
                target_node = record["m"]
                target_id = target_node.id
                if target_id not in nodes:
                    target_data = dict(target_node.items())
                    target_data["id"] = target_id
                    target_data["labels"] = list(target_node.labels)
                    nodes[target_id] = target_data
                
                # Add relationship
                relationship = record["r"]
                rel_data = {
                    "id": relationship.id,
                    "type": relationship.type,
                    "properties": dict(relationship.items()),
                    "start_node": relationship.start_node.id,
                    "end_node": relationship.end_node.id
                }
                relationships.append(rel_data)
            
            return {
                "nodes": list(nodes.values()),
                "relationships": relationships
            }
            
        except Exception as e:
            import traceback
            print(f"Error finding view node connections: {str(e)}")
            print(traceback.format_exc())
            return {"nodes": [], "relationships": []} 