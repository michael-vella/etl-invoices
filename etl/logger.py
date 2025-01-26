import logging

def get_logger(name: str="Logger") -> logging.Logger:
    """Creates and returns logger instance."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    
    return logging.getLogger(name)
