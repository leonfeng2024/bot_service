import logging
import os
import sys
import yaml
from logging.config import dictConfig

def get_logger(name):
    """
    Creates and returns a logger with the given name.
    
    Args:
        name: Name for the logger, usually __name__
        
    Returns:
        A configured logger
    """
    # 确保logs目录存在
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # 加载日志配置文件
    config_path = os.path.join(logs_dir, 'logging_config.yaml')
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            # 更新日志文件路径为绝对路径
            config['handlers']['file']['filename'] = os.path.join(logs_dir, 'app.log')
            dictConfig(config)
    else:
        # 如果配置文件不存在，使用默认配置
        logging.basicConfig(level=logging.INFO)
    
    return logging.getLogger(name)