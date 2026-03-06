import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from .logger import setup_logger

logger = setup_logger(__name__)

class DatabaseLoader:
    """Class handling database connections and insertions."""
    
    def __init__(self):
        load_dotenv()
        self.host = os.environ.get('DB_HOST')
        self.port = os.environ.get('DB_PORT')
        self.db = os.environ.get('DB_NAME')
        self.user = os.environ.get('DB_USER')
        self.password = os.environ.get('DB_PASSWORD')

        # Fail fast if essential credentials are not provided via .env
        missing_vars = [var for var, val in zip(['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD'], 
                                                [self.host, self.db, self.user, self.password]) if not val]
        
        if missing_vars:
            logger.error(f"Missing required database environment variables: {', '.join(missing_vars)}")
            raise ValueError(f"Missing required database environment variables: {', '.join(missing_vars)}")
        
    def _get_engine(self):
        try:
            # URL format: postgresql+psycopg2://user:password@host:port/dbname
            connection_url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
            engine = create_engine(connection_url)
            return engine
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise
            
    def load_table(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
        logger.info(f"Loading '{table_name}' to database '{self.db}' on '{self.host}' in 'staging' schema")
        try:
            engine = self._get_engine()
            
            # Ensure the staging schema exists before inserting
            with engine.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
                conn.commit()

            df.to_sql(name=table_name, con=engine, schema='staging', if_exists=if_exists, index=False)
            logger.info(f"Successfully loaded {len(df)} rows into staging.{table_name}")
        except Exception as e:
            logger.error(f"Failed to insert table {table_name}: {e}")
