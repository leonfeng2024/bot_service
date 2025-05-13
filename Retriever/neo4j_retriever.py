from typing import List, Dict, Any
from Retriever.base_retriever import BaseRetriever
from service.neo4j_service import Neo4jService
from service.llm_service import LLMService
from tools.redis_tools import RedisTools
from tools.token_counter import TokenCounter
import json

class Neo4jRetriever(BaseRetriever):
    """Neo4j retriever for retrieving data from Neo4j graph database"""
    
    def __init__(self):
        """Initialize Neo4j retriever"""
        self.neo4j_service = Neo4jService()
        self.llm_service = LLMService()
        self.redis_tools = RedisTools()
        # Initialize token counter
        self.token_counter = TokenCounter()
    
    async def _query_relationships(self, term: str) -> List[Dict[str, Any]]:
        """
        Query all relationships related to the specified term
        
        Args:
            term: The term to query
            
        Returns:
            List containing relationship information
        """
        try:
            # Check Neo4j connection status
            if not self.neo4j_service.neo4j.connected:
                print(f"Neo4j is not connected, trying to reconnect...")
                self.neo4j_service.neo4j._connect()  # Try to reconnect
                if not self.neo4j_service.neo4j.connected:
                    return [{"content": "Neo4j database connection failed, please check configuration", "score": 0.0}]
            
            # First try to query all properties of relationships for diagnosis
            debug_query = """
            MATCH (a:Table)-[r:RELATED_TO]->(b:Table)
            WHERE a.name = $term OR b.name = $term
            RETURN 
                a.name as source_table,
                b.name as target_table,
                properties(r) as relationship_properties,
                type(r) as relationship_type
            LIMIT 5
            """
            
            debug_results = self.neo4j_service.neo4j.execute_query(debug_query, parameters={"term": term})
            if debug_results:
                for record in debug_results:
                    print(f"DEBUG - Relationship properties for term '{term}': {record.get('relationship_properties')}")
                    print(f"DEBUG - Relationship type: {record.get('relationship_type')}")
            else:
                print(f"DEBUG - No relationships found for term '{term}' in initial diagnostic query")
                
                # Check if table exists
                table_exists_query = """
                MATCH (t:Table)
                WHERE t.name = $term
                RETURN t.name as table_name
                """
                table_exists = self.neo4j_service.neo4j.execute_query(table_exists_query, parameters={"term": term})
                if not table_exists:
                    print(f"DEBUG - Table '{term}' does not exist in Neo4j")
                    
                    # Try to find tables containing this term
                    partial_match_query = """
                    MATCH (t:Table)
                    WHERE t.name CONTAINS $term
                    RETURN t.name as table_name
                    LIMIT 5
                    """
                    partial_matches = self.neo4j_service.neo4j.execute_query(partial_match_query, parameters={"term": term})
                    if partial_matches:
                        matched_tables = [record.get('table_name') for record in partial_matches if record.get('table_name')]
                        print(f"DEBUG - Found tables containing '{term}': {matched_tables}")
                    else:
                        print(f"DEBUG - No tables containing '{term}' found")
            
            # Modified Cypher query to get more possible field names
            cypher_query = """
            MATCH (a:Table)-[r:RELATED_TO]->(b:Table)
            WHERE a.name = $term OR b.name = $term OR a.name CONTAINS $term OR b.name CONTAINS $term
            RETURN 
                a.name as source_table,
                b.name as target_table,
                r.source_field as source_field,
                r.target_field as target_field,
                r.sourceField as source_field_alt,
                r.targetField as target_field_alt,
                r.from_field as from_field,
                r.to_field as to_field,
                r.relationship_description as description,
                r.relationshipDescription as description_alt,
                r.created_at as created_at,
                r.createdAt as created_at_alt
            ORDER BY COALESCE(r.created_at, r.createdAt) DESC
            LIMIT 20
            """
            
            # Execute query
            results = self.neo4j_service.neo4j.execute_query(cypher_query, parameters={"term": term})
            print(f"Neo4j query results for term '{term}': {len(results)} records found")
            
            # Format results
            formatted_results = []
            for record in results:
                try:
                    # Print full record for diagnosis
                    print(f"DEBUG - Record: {record}")
                    
                    # Get table names
                    source_table = record.get('source_table', 'Unknown')
                    target_table = record.get('target_table', 'Unknown')
                    
                    # Try different field names to get source field
                    source_field = (
                        record.get('source_field') or 
                        record.get('source_field_alt') or 
                        record.get('from_field') or 
                        'Unknown'
                    )
                    
                    # Try different field names to get target field
                    target_field = (
                        record.get('target_field') or 
                        record.get('target_field_alt') or 
                        record.get('to_field') or 
                        'Unknown'
                    )
                    
                    # Get description and creation time
                    description = record.get('description') or record.get('description_alt') or ''
                    created_at = record.get('created_at') or record.get('created_at_alt') or ''
                    
                    content_message = "Table {} is related to table {} through field {} to field {}".format(
                        source_table, target_table, source_field, target_field
                    )
                    
                    formatted_results.append({
                        "content": content_message,
                        "description": description,
                        "created_at": created_at,
                        "score": 1.0,
                        "source": "neo4j"
                    })
                except Exception as record_error:
                    print(f"Error processing record: {record}, Error: {str(record_error)}")
                    continue
            
            if not formatted_results:
                print(f"No relationships found for term '{term}', trying broader query...")
                
                # If no results, try broader query to find any relationships
                broader_query = """
                MATCH (a:Table)-[r:RELATED_TO]->(b:Table)
                RETURN 
                    a.name as source_table,
                    b.name as target_table,
                    r.source_field as source_field,
                    r.target_field as target_field
                LIMIT 10
                """
                
                broader_results = self.neo4j_service.neo4j.execute_query(broader_query)
                if broader_results:
                    print(f"Found {len(broader_results)} relationships with broader query")
                    formatted_results.append({
                        "content": f"No direct table relationships found for '{term}'",
                        "description": "Example relationships in database",
                        "score": 0.0,
                        "source": "neo4j"
                    })
                else:
                    # If no results, try to query existing tables
                    table_query = """
                    MATCH (t:Table)
                    RETURN t.name as table_name
                    LIMIT 10
                    """
                    
                    table_results = self.neo4j_service.neo4j.execute_query(table_query)
                    if table_results:
                        tables = [record.get('table_name') for record in table_results if record.get('table_name')]
                        if tables:
                            formatted_results.append({
                                "content": f"Found these tables: {', '.join(tables)}, but no relationships with '{term}'",
                                "description": "Found tables but no relationships",
                                "score": 0.1,
                                "source": "neo4j"
                            })
                    else:
                        formatted_results.append({
                            "content": f"No tables or relationships found in Neo4j database",
                            "description": "Empty database",
                            "score": 0.0,
                            "source": "neo4j"
                        })
            
            return formatted_results
            
        except Exception as e:
            print(f"Error querying relationships for term '{term}': {str(e)}")
            print(f"Full error details: {e.__class__.__name__}: {str(e)}")
            return [{"content": f"Error querying Neo4j: {str(e)}", "score": 0.0, "source": "neo4j"}]
    
    async def retrieve(self, query: str, uuid: str = None) -> List[Dict[str, Any]]:
        """
        Retrieve relevant relationships based on user query and cache results
        
        Args:
            query: User query string
            uuid: User UUID for caching results
            
        Returns:
            List containing relevant relationship information
        """
        try:
            # Record token usage at start
            start_usage = self.llm_service.get_token_usage()
            
            # Build and print prompt
            prompt = f"Neo4j retrieval query:\n{query}"
            print(prompt)
            
            # Call LLM to analyze query and identify intent
            # Note: identify_column is an async generator, needs proper handling
            intent_analysis = {}
            try:
                async for item in self.llm_service.identify_column(query):
                    # Skip if item is not a dictionary
                    if not isinstance(item, dict):
                        continue
                    
                    # If it's a status message, print it
                    if 'step' in item and 'message' in item:
                        print(f"Identification status: {item['message']}")
                        continue
                    
                    # Otherwise, this is the result dictionary
                    intent_analysis = item
                    break  # Only take first result
                
                print(f"Identified intent: {intent_analysis}")
            except Exception as intent_error:
                print(f"Error identifying intent: {str(intent_error)}")
                # If identification fails, continue with empty dict
            
            all_results = []
            
            # If intent was identified, search for each key term
            if intent_analysis:
                for key, term in intent_analysis.items():
                    relationship_results = await self._query_relationships(term)
                    all_results.extend(relationship_results)
            else:
                # If no intent identified, use original query directly
                all_results.extend(await self._query_relationships(query))
            
            # Record token usage at end
            end_usage = self.llm_service.get_token_usage()
            
            # Calculate tokens consumed in this call
            input_tokens = end_usage["input_tokens"] - start_usage["input_tokens"]
            output_tokens = end_usage["output_tokens"] - start_usage["output_tokens"]
            
            # Print token usage
            print(f"[Neo4j Retriever] Total token usage - Input: {input_tokens} tokens, Output: {output_tokens} tokens")
            
            # If no results, return not found message
            if not all_results:
                all_results = [{"content": "No table relationships found for the query", "score": 0, "source": "neo4j"}]
            
            # Add token usage info to results
            for result in all_results:
                result["token_usage"] = {
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens
                }
            
            # If UUID provided, cache results in Redis
            if uuid:
                try:
                    # Get existing cached data
                    cached_data = self.redis_tools.get(uuid) or {}
                    
                    # Save results to cache
                    cached_data["neo4j"] = all_results
                    
                    # Update Redis cache
                    self.redis_tools.set(uuid, cached_data)
                    print(f"Cached Neo4j results for UUID: {uuid}")
                    
                    # Set neo4j key separately to ensure accessibility by other components
                    self.redis_tools.set(f"{uuid}:neo4j", all_results)
                    print(f"Cached Neo4j results to separate key: {uuid}:neo4j")
                except Exception as cache_error:
                    print(f"Error caching Neo4j results: {str(cache_error)}")
            
            return all_results
            
        except Exception as e:
            print(f"Neo4j retrieval error: {str(e)}")
            import traceback
            print(f"Detailed error: {traceback.format_exc()}")
            
            error_result = [{"content": f"Error querying Neo4j: {str(e)}", "score": 0, "source": "neo4j"}]
            
            # If UUID provided, store error result in Redis
            if uuid:
                try:
                    # Get existing cached data
                    cached_data = self.redis_tools.get(uuid) or {}
                    # Set error result
                    cached_data["neo4j"] = error_result
                    # Update Redis cache
                    self.redis_tools.set(uuid, cached_data)
                    # Set neo4j key separately
                    self.redis_tools.set(f"{uuid}:neo4j", error_result)
                except Exception as cache_error:
                    print(f"Error caching Neo4j error results: {str(cache_error)}")
                
            return error_result
        finally:
            try:
                self.neo4j_service.close()
            except Exception as close_error:
                pass