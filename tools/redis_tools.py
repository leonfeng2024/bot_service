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
        初始化Redis连接
        """
        self.redis_client = None
        try:
            # 准备Redis连接参数
            redis_params = {
                "host": REDIS_HOST,
                "port": REDIS_PORT,
                "db": REDIS_DATABASE,
                "socket_timeout": REDIS_TIMEOUT,
                "socket_connect_timeout": REDIS_CONNECT_TIMEOUT,
                "client_name": REDIS_CLIENT_NAME,
                "decode_responses": True  # 自动将响应解码为字符串
            }
            
            # 只有当密码不为None时才添加密码参数
            if REDIS_PASSWORD is not None:
                redis_params["password"] = REDIS_PASSWORD
            
            self.redis_client = redis.Redis(**redis_params)
            
            # 测试连接
            self.redis_client.ping()
            logger.info(f"Redis connection established successfully to {REDIS_HOST}:{REDIS_PORT}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            logger.warning("Running with Redis disabled. Cache operations will be no-ops.")
            # 确保客户端为None
            self.redis_client = None

    def set(self, key: str, value: Dict[str, Any], expire: int = JWT_EXPIRATION) -> bool:
        """
        设置key-value并设置过期时间，默认与JWT令牌过期时间相同
        
        Args:
            key: 键（通常是用户的UUID）
            value: 要存储的值（字典）
            expire: 过期时间（秒）
            
        Returns:
            bool: 操作是否成功
        """
        if not self.redis_client:
            logger.warning(f"Redis not connected. Set operation skipped for key: {key}")
            return False
            
        try:
            # 将字典转换为JSON字符串
            json_value = json.dumps(value)
            # 设置键值对
            self.redis_client.set(key, json_value)
            # 设置过期时间
            self.redis_client.expire(key, expire)
            logger.info(f"Successfully set key in Redis: {key}")
            return True
        except Exception as e:
            logger.error(f"Error setting value in Redis: {e}")
            return False

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        获取指定key的值
        
        Args:
            key: 键（通常是用户的UUID）
            
        Returns:
            Optional[Dict]: 返回存储的字典，如果key不存在则返回None
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
        删除指定key
        
        Args:
            key: 键（通常是用户的UUID）
            
        Returns:
            bool: 操作是否成功
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
        编辑已存在key中的特定字段
        
        Args:
            key: 键（通常是用户的UUID）
            field: 要更新的字段名
            value: 新的字段值
            
        Returns:
            bool: 操作是否成功
        """
        if not self.redis_client:
            logger.warning("Redis not connected. Edit operation skipped.")
            return False
            
        try:
            # 获取当前数据
            current_data = self.get(key)
            if not current_data:
                logger.warning(f"Key {key} not found for editing")
                return False
            
            # 更新字段
            current_data[field] = value
            
            # 获取当前的TTL（过期时间）
            ttl = self.redis_client.ttl(key)
            if ttl < 0:  # 如果键不存在或没有过期时间
                ttl = JWT_EXPIRATION
                
            # 保存更新后的数据，保持原有的过期时间
            return self.set(key, current_data, ttl)
        except Exception as e:
            logger.error(f"Error editing key in Redis: {e}")
            return False 