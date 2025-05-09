import redis
import json
from typing import Dict, Any, List, Optional, Union
import logging
from config import (
    REDIS_HOST, 
    REDIS_PORT, 
    REDIS_PASSWORD, 
    REDIS_DATABASE, 
    REDIS_TIMEOUT,
    REDIS_CONNECT_TIMEOUT,
    REDIS_CLIENT_NAME,
    JWT_EXPIRATION
)

logger = logging.getLogger(__name__)

class RedisTools:
    def __init__(self):
        """
        Initialize Redis connection
        """
        self.redis_client = None
        try:
            # Prepare Redis connection parameters
            redis_params = {
                "host": REDIS_HOST,
                "port": REDIS_PORT,
                "db": REDIS_DATABASE,
                "socket_timeout": REDIS_TIMEOUT,
                "socket_connect_timeout": REDIS_CONNECT_TIMEOUT,
                "client_name": REDIS_CLIENT_NAME,
                "decode_responses": True  # Automatically decode responses to strings
            }
            
            # Only add password parameter if it's not None
            if REDIS_PASSWORD is not None:
                redis_params["password"] = REDIS_PASSWORD
            
            self.redis_client = redis.Redis(**redis_params)
            
            # Test connection
            self.redis_client.ping()
            logger.info(f"Redis connection established successfully to {REDIS_HOST}:{REDIS_PORT}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            logger.warning("Running with Redis disabled. Cache operations will be no-ops.")
            # Ensure client is None
            self.redis_client = None

    def set(self, key: str, value: Dict[str, Any], expire: int = JWT_EXPIRATION) -> bool:
        """
        Set key-value pair with expiration time, defaults to JWT token expiration time
        
        Args:
            key: Key (usually user's UUID)
            value: Value to store (dictionary)
            expire: Expiration time (seconds)
            
        Returns:
            bool: Whether operation was successful
        """
        if not self.redis_client:
            logger.warning(f"Redis not connected. Set operation skipped for key: {key}")
            return False
            
        try:
            # Convert dictionary to JSON string
            json_value = json.dumps(value)
            # Set key-value pair
            self.redis_client.set(key, json_value)
            # Set expiration time
            self.redis_client.expire(key, expire)
            logger.info(f"Successfully set key in Redis: {key}")
            return True
        except Exception as e:
            logger.error(f"Error setting value in Redis: {e}")
            return False

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get value for specified key
        
        Args:
            key: Key (usually user's UUID)
            
        Returns:
            Optional[Dict]: Stored dictionary, or None if key doesn't exist
        """
        if not self.redis_client:
            logger.warning("Redis not connected. Get operation skipped.")
            return None
            
        try:
            value = self.redis_client.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"Error getting value from Redis: {e}")
            return None

    def delete(self, key: str) -> bool:
        """
        Delete specified key
        
        Args:
            key: Key (usually user's UUID)
            
        Returns:
            bool: Whether operation was successful
        """
        if not self.redis_client:
            logger.warning("Redis not connected. Delete operation skipped.")
            return False
            
        try:
            result = self.redis_client.delete(key)
            return result > 0
        except Exception as e:
            logger.error(f"Error deleting key from Redis: {e}")
            return False

    def edit(self, key: str, field: str, value: Union[str, List, Dict]) -> bool:
        """
        Edit specific field in existing key
        
        Args:
            key: Key (usually user's UUID)
            field: Field name to update
            value: New field value
            
        Returns:
            bool: Whether operation was successful
        """
        if not self.redis_client:
            logger.warning("Redis not connected. Edit operation skipped.")
            return False
            
        try:
            # Get current data
            current_data = self.get(key)
            if not current_data:
                logger.warning(f"Key {key} not found for editing")
                return False
            
            # Update field
            current_data[field] = value
            
            # Get current TTL (expiration time)
            ttl = self.redis_client.ttl(key)
            if ttl < 0:  # If key doesn't exist or has no expiration time
                ttl = JWT_EXPIRATION
                
            # Save updated data, maintaining original expiration time
            return self.set(key, current_data, ttl)
        except Exception as e:
            logger.error(f"Error editing key in Redis: {e}")
            return False 