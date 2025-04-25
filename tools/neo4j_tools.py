from neo4j import GraphDatabase
from typing import Dict, Any, List, Optional
import traceback
import config
import os
import socket


class Neo4jTools:
    def __init__(self):
        """
        Initialize Neo4j client with configuration from config.py.
        """
        self.driver = None
        self.connected = False
        self._connect()

    def _connect(self) -> None:
        """Establish connection to Neo4j database."""
        try:
            # First attempt with configuration from config.py
            print(f"Attempting to connect to Neo4j using URI: {config.NEO4J_URI}")
            self.driver = GraphDatabase.driver(
                config.NEO4J_URI,
                auth=(config.NEO4J_USERNAME, config.NEO4J_PASSWORD)
            )
            # Test the connection
            with self.driver.session() as session:
                result = session.run("RETURN 1")
                result.single()
            print("Successfully connected to Neo4j database")
            self.connected = True
        except Exception as e:
            print(f"Error connecting to Neo4j: {str(e)}")
            
            # Try with localhost instead of container name
            try:
                local_uri = f"bolt://localhost:{config.NEO4J_BOLT_PORT}"
                print(f"Attempting to connect to Neo4j using local URI: {local_uri}")
                self.driver = GraphDatabase.driver(
                    local_uri,
                    auth=(config.NEO4J_USERNAME, config.NEO4J_PASSWORD)
                )
                # Test the connection
                with self.driver.session() as session:
                    result = session.run("RETURN 1")
                    result.single()
                print("Successfully connected to Neo4j database using localhost")
                self.connected = True
            except Exception as local_e:
                print(f"Error connecting to Neo4j locally: {str(local_e)}")
                
                # Try with IP address 127.0.0.1
                try:
                    ip_uri = f"bolt://127.0.0.1:{config.NEO4J_BOLT_PORT}"
                    print(f"Attempting to connect to Neo4j using IP URI: {ip_uri}")
                    self.driver = GraphDatabase.driver(
                        ip_uri,
                        auth=(config.NEO4J_USERNAME, config.NEO4J_PASSWORD)
                    )
                    # Test the connection
                    with self.driver.session() as session:
                        result = session.run("RETURN 1")
                        result.single()
                    print("Successfully connected to Neo4j database using IP address")
                    self.connected = True
                except Exception as ip_e:
                    print(f"Error connecting to Neo4j using IP: {str(ip_e)}")
                    print("Creating a mock Neo4j driver for development purposes")
                    self.connected = False

    def close(self) -> None:
        """Close the Neo4j connection."""
        if self.driver and self.connected:
            self.driver.close()

    def execute_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a Cypher query and return the results.
        
        Args:
            query: Cypher query to execute
            parameters: Optional parameters for the query
            
        Returns:
            List of dictionaries containing query results
        """
        if not self.connected:
            print("Not connected to Neo4j, returning empty results")
            return []
            
        try:
            with self.driver.session() as session:
                result = session.run(query, parameters or {})
                return [dict(record) for record in result]
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            print(traceback.format_exc())
            return []

    def get_node_count(self) -> int:
        """
        Get the total number of nodes in the database.
        
        Returns:
            Total number of nodes
        """
        if not self.connected:
            return 0
            
        try:
            with self.driver.session() as session:
                result = session.run("MATCH (n) RETURN count(n) as count")
                return result.single()["count"]
        except Exception as e:
            print(f"Error getting node count: {str(e)}")
            print(traceback.format_exc())
            return 0

    def get_relationship_count(self) -> int:
        """
        Get the total number of relationships in the database.
        
        Returns:
            Total number of relationships
        """
        if not self.connected:
            return 0
            
        try:
            with self.driver.session() as session:
                result = session.run("MATCH ()-[r]->() RETURN count(r) as count")
                return result.single()["count"]
        except Exception as e:
            print(f"Error getting relationship count: {str(e)}")
            print(traceback.format_exc())
            return 0

    def get_node_labels(self) -> List[str]:
        """
        Get all node labels in the database.
        
        Returns:
            List of node labels
        """
        if not self.connected:
            return []
            
        try:
            with self.driver.session() as session:
                result = session.run("CALL db.labels()")
                return [record["label"] for record in result]
        except Exception as e:
            print(f"Error getting node labels: {str(e)}")
            print(traceback.format_exc())
            return []

    def get_relationship_types(self) -> List[str]:
        """
        Get all relationship types in the database.
        
        Returns:
            List of relationship types
        """
        if not self.connected:
            return []
            
        try:
            with self.driver.session() as session:
                result = session.run("CALL db.relationshipTypes()")
                return [record["relationshipType"] for record in result]
        except Exception as e:
            print(f"Error getting relationship types: {str(e)}")
            print(traceback.format_exc())
            return []

    def create_node(
        self,
        label: str,
        properties: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Create a new node with the given label and properties.
        
        Args:
            label: Node label
            properties: Node properties
            
        Returns:
            Created node data if successful, None otherwise
        """
        if not self.connected:
            return None
            
        try:
            query = f"CREATE (n:{label} $props) RETURN n"
            with self.driver.session() as session:
                result = session.run(query, props=properties)
                return dict(result.single()["n"])
        except Exception as e:
            print(f"Error creating node: {str(e)}")
            print(traceback.format_exc())
            return None

    def create_relationship(
        self,
        start_node_label: str,
        start_node_props: Dict[str, Any],
        end_node_label: str,
        end_node_props: Dict[str, Any],
        relationship_type: str,
        relationship_props: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Create a relationship between two nodes.
        
        Args:
            start_node_label: Label of the start node
            start_node_props: Properties to identify the start node
            end_node_label: Label of the end node
            end_node_props: Properties to identify the end node
            relationship_type: Type of the relationship
            relationship_props: Optional properties for the relationship
            
        Returns:
            True if successful, False otherwise
        """
        if not self.connected:
            return False
            
        try:
            query = f"""
            MATCH (a:{start_node_label}), (b:{end_node_label})
            WHERE a = $start_props AND b = $end_props
            CREATE (a)-[r:{relationship_type} $rel_props]->(b)
            RETURN r
            """
            with self.driver.session() as session:
                result = session.run(
                    query,
                    start_props=start_node_props,
                    end_props=end_node_props,
                    rel_props=relationship_props or {}
                )
                return result.single() is not None
        except Exception as e:
            print(f"Error creating relationship: {str(e)}")
            print(traceback.format_exc())
            return False


if __name__ == "__main__":
    # Initialize Neo4j client with config from config.py
    neo4j = Neo4jTools()
    
    # Test basic operations
    print("\nTesting basic operations...")
    print(f"Total nodes: {neo4j.get_node_count()}")
    print(f"Total relationships: {neo4j.get_relationship_count()}")
    print(f"Node labels: {neo4j.get_node_labels()}")
    print(f"Relationship types: {neo4j.get_relationship_types()}")
    
    # Close connection
    neo4j.close() 