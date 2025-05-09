import logging
from datetime import datetime
from typing import Dict, List, Optional
from tools.postgresql_tools import PostgreSQLTools
import asyncio

logger = logging.getLogger(__name__)

class UserService:
    def __init__(self):
        self.pg_tools = PostgreSQLTools()
    
    async def get_user_profile(self, user_id: str) -> Optional[Dict]:
        """
        Get user profile information
        
        Args:
            user_id: User ID (string type)
            
        Returns:
            User profile information or None if user not found
        """
        try:
            query = """
            SELECT 
                user_id as id, 
                username, 
                email, 
                role
            FROM user_info 
            WHERE user_id = :user_id
            """
            
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.pg_tools.execute_query(query, {"user_id": user_id})
            )
            
            if result and len(result) > 0:
                return result[0]
            return None
            
        except Exception as e:
            logger.error(f"Error getting user profile: {str(e)}")
            return None
    
    async def get_all_users(self) -> List[Dict]:
        """
        Get all users in the system
        
        Returns:
            List of user information
        """
        try:
            query = """
            SELECT 
                user_id as id, 
                username, 
                email, 
                role
            FROM user_info
            ORDER BY user_id
            """
            
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.pg_tools.execute_query(query)
            )
            
            return result or []
            
        except Exception as e:
            logger.error(f"Error getting all users: {str(e)}")
            return []
    
    async def create_user(self, username: str, email: str, password: str, role: str) -> Dict:
        """
        Create a new user
        
        Args:
            username: Username (will also be used as user_id)
            email: Email address
            password: Password
            role: User role
            
        Returns:
            Dictionary with success status, message, and user info
        """
        try:
            # First check if username or email already exists
            check_query = """
            SELECT count(*) as count
            FROM user_info
            WHERE username = :username OR email = :email
            """
            
            check_result = self.pg_tools.execute_query(check_query, {
                "username": username,
                "email": email
            })
            
            if check_result and check_result[0]["count"] > 0:
                return {
                    "success": False,
                    "message": "Username or email already exists",
                    "user": None
                }
            
            # Create new user
            create_query = """
            INSERT INTO user_info (user_id, username, email, password, role, isactive)
            VALUES (:user_id, :username, :email, :password, :role, true)
            RETURNING user_id as id, username, email, role
            """
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.pg_tools.execute_query(create_query, {
                    "user_id": username,
                    "username": username,
                    "email": email,
                    "password": password,
                    "role": role
                })
            )
            
            # If we reach here without raising an exception, SQL execution was successful
            # Prepare user data to return
            user_data = {}
            if result and isinstance(result, list) and len(result) > 0:
                user_data = result[0]
            else:
                # SQL executed successfully but no data returned, construct basic user data
                user_data = {
                    "id": username,
                    "username": username,
                    "email": email,
                    "role": role
                }
            
            return {
                "success": True,
                "message": "User created successfully",
                "user": user_data
            }
            
        except Exception as e:
            logger.error(f"Error creating user: {str(e)}")
            return {
                "success": False,
                "message": f"Error creating user: {str(e)}",
                "user": None
            }
    
    async def update_user(self, user_id: str, update_data: Dict) -> Dict:
        """
        Update user information
        
        Args:
            user_id: User ID (string type)
            update_data: Dictionary with fields to update
            
        Returns:
            Dictionary with success status, message, and updated user info
        """
        try:
            # First check if user exists
            check_query = """
            SELECT count(*) as count
            FROM user_info
            WHERE user_id = :user_id
            """
            
            loop = asyncio.get_running_loop()
            check_result = await loop.run_in_executor(
                None,
                lambda: self.pg_tools.execute_query(check_query, {"user_id": user_id})
            )
            
            if not check_result or check_result[0]["count"] == 0:
                return {
                    "success": False,
                    "message": "User does not exist",
                    "user": None
                }
            
            # Build update query
            set_parts = []
            params = {"user_id": user_id}
            
            if "username" in update_data and update_data["username"]:
                set_parts.append("username = :username")
                params["username"] = update_data["username"]
                
            if "email" in update_data and update_data["email"]:
                set_parts.append("email = :email")
                params["email"] = update_data["email"]
                
            if "role" in update_data and update_data["role"]:
                set_parts.append("role = :role")
                params["role"] = update_data["role"]
                
            if "password" in update_data and update_data["password"]:
                set_parts.append("password = :password")
                params["password"] = update_data["password"]
            
            if not set_parts:
                return {
                    "success": False,
                    "message": "No fields to update",
                    "user": None
                }
            
            # Create and execute update query
            update_query = f"""
            UPDATE user_info
            SET {", ".join(set_parts)}
            WHERE user_id = :user_id
            RETURNING user_id as id, username, email, role
            """
            
            result = await loop.run_in_executor(
                None,
                lambda: self.pg_tools.execute_query(update_query, params)
            )
            
            # If we reach here without raising an exception, SQL execution was successful
            # Prepare user data to return
            user_data = {}
            if result and len(result) > 0:
                user_data = result[0]
            else:
                # SQL executed successfully but no data returned, construct basic user data
                user_data = {
                    "id": user_id,
                    "username": update_data.get("username", ""),
                    "email": update_data.get("email", ""),
                    "role": update_data.get("role", "")
                }
            
            return {
                "success": True,
                "message": "User updated successfully",
                "user": user_data
            }
            
        except Exception as e:
            logger.error(f"Error updating user: {str(e)}")
            return {
                "success": False,
                "message": f"Error updating user: {str(e)}",
                "user": None
            }
    
    async def delete_user(self, user_id: str) -> Dict:
        """
        Delete a user
        
        Args:
            user_id: User ID (string type)
            
        Returns:
            Dictionary with success status and message
        """
        try:
            # First check if user exists
            check_query = """
            SELECT count(*) as count
            FROM user_info
            WHERE user_id = :user_id
            """
            loop = asyncio.get_running_loop()
            check_result = await loop.run_in_executor(
                None,
                lambda: self.pg_tools.execute_query(check_query, {"user_id": user_id})
            )
            if not check_result or check_result[0]["count"] == 0:
                logger.info(f"User {user_id} does not exist, cannot delete.")
                return {
                    "success": False,
                    "message": "User does not exist"
                }
            # Delete user
            delete_query = """
            DELETE FROM user_info
            WHERE user_id = :user_id
            RETURNING user_id
            """
            
            result = await loop.run_in_executor(
                None,
                lambda: self.pg_tools.execute_query(delete_query, {"user_id": user_id})
            )
            logger.info(f"Delete user result for {user_id}: {result}")
            
            # If we reach here without raising an exception, SQL execution was successful
            return {
                "success": True,
                "message": "User deleted successfully"
            }
        except Exception as e:
            logger.error(f"Error deleting user: {str(e)}")
            return {
                "success": False,
                "message": f"Error deleting user: {str(e)}"
            } 