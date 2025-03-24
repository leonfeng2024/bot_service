import logging
import os
import sys

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import LOGGING_LEVEL, LOGGING_FORMAT

def get_logger(name):
    """
    Creates and returns a logger with the given name.
    
    Args:
        name: Name for the logger, usually __name__
        
    Returns:
        A configured logger
    """
    logger = logging.getLogger(name)
    
    # Set log level from config
    log_level = getattr(logging, LOGGING_LEVEL, logging.INFO)
    logger.setLevel(log_level)
    
    # Create handler if logger doesn't have handlers
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)
        
        # Create formatter using format from config
        formatter = logging.Formatter(LOGGING_FORMAT)
        handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(handler)
    
    return logger 