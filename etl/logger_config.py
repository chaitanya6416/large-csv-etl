import os 
import logging 
from logging.handlers import RotatingFileHandler

def setup_logging():
    """
    Configures the logging for the application.
    Logs to both a file and the console.
    """
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "etl.log")

    # Get root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Avoid adding handlers multiple times
    if logger.hasHandlers():
        logger.handlers.clear()

    # Console handler
    c_handler = logging.StreamHandler()
    c_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)

    # File handler
    f_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)
    
    return logger
