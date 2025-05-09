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
    # Ensure logs directory exists
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Load logging configuration file
    config_path = os.path.join(logs_dir, 'logging_config.yaml')
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            # Update log file path to absolute path
            config['handlers']['file']['filename'] = os.path.join(logs_dir, 'app.log')
            dictConfig(config)
    else:
        # Use default configuration if config file doesn't exist
        logging.basicConfig(level=logging.INFO)
    
    return logging.getLogger(name)