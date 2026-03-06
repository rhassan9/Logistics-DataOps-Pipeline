import os
import pandas as pd
from .logger import setup_logger

logger = setup_logger(__name__)

class DataLoader:
    """Class responsible for safely extracting CSV data."""
    
    def __init__(self, raw_data_dir: str):
        self.raw_data_dir = raw_data_dir
        
    def load_csv(self, filename: str) -> pd.DataFrame:
        """
        Dynamically ingests a CSV file into a pandas dataframe.
        Wraps extraction in a try-except to catch missing files or errors.
        """
        filepath = os.path.join(self.raw_data_dir, filename)
        try:
            logger.info(f"Attempting to load data from {filepath}")
            df = pd.read_csv(filepath)
            logger.info(f"Successfully loaded {filename}. Shape: {df.shape}")
            return df
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}. Please ensure the dataset exists.")
            # Return an empty dataframe to prevent complete pipeline crash, or raise depending on preference.
            # We raise here because missing critical tables (like trips) should halt execution.
            raise
        except pd.errors.EmptyDataError:
            logger.error(f"File is empty: {filepath}.")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while loading {filepath}: {e}")
            raise
