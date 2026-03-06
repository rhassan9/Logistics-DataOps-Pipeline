import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logger(name="ETLPipeline", log_file="pipeline.log", level=logging.INFO):
    """
    Sets up a logger with file and console handlers.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Return logger if already configured (prevents duplicate logs)
    if logger.handlers:
        return logger

    # Format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # File Handler (rotating log, max 5MB, keep 3 backups)
    file_handler = RotatingFileHandler(
        log_file, maxBytes=5*1024*1024, backupCount=3
    )
    file_handler.setFormatter(formatter)

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
