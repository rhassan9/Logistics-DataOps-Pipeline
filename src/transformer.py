import pandas as pd
import numpy as np
from .logger import setup_logger

logger = setup_logger(__name__)

class DataTransformer:
    """Class responsible for safely cleaning and transforming data."""
    
    def __init__(self):
        pass

    def standardize_timestamps(self, df: pd.DataFrame, time_columns: list) -> pd.DataFrame:
        """
        Converts diverse time formats (Unix epoch, broken 1970 strings) to standard ISO 8601 datetimes.
        """
        logger.info(f"Standardizing timestamps for columns: {time_columns}")
        for col in time_columns:
            if col in df.columns:
                # pandas to_datetime handles both valid strings and unix timestamps (unit='s') dynamically.
                # However, if it's mixed, we coerce errors to NaT, then handle logic
                
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col], unit='s', errors='coerce')
                else:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # Further cleanup: anything exactly '1970-01-01' we convert to NaT as it's a known default/broken date
                df.loc[df[col] == '1970-01-01', col] = pd.NaT

        return df

    def flag_missing_telematics(self, df: pd.DataFrame, target_column: str, flag_name: str = "is_telematics_drop") -> pd.DataFrame:
        """
        Flags missing business critical values instead of dropping rows.
        Creates a new boolean column indicating the drop.
        """
        logger.info(f"Flagging missing values in {target_column} as {flag_name}")
        if target_column in df.columns:
            df[flag_name] = df[target_column].isna()
        else:
            logger.warning(f"Target column {target_column} not found in dataframe.")
            df[flag_name] = False
        return df

    def cast_data_types(self, df: pd.DataFrame, type_mapping: dict) -> pd.DataFrame:
        """
        Explicitly casts columns to required types (e.g. dimensions to strings, facts to floats).
        type_mapping: dictionary of column_name: target_type (e.g., {'truck_id': 'str', 'cost': 'float64'})
        """
        logger.info("Casting data types according to mapping.")
        for col, dtype in type_mapping.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except ValueError as e:
                    logger.error(f"Failed to cast column {col} to {dtype}: {e}")
            else:
                logger.warning(f"Column {col} not found for type casting.")
        return df

    def handle_missing_entities(self, df: pd.DataFrame, entity_columns: dict) -> pd.DataFrame:
        """
        Fills missing operational entity IDs with 'UNKNOWN_<ENTITY>' to maintain referential integrity.
        entity_columns: dict of column_name: unknown_label (e.g., {'driver_id': 'UNKNOWN_DRIVER'})
        """
        logger.info(f"Handling missing entities for columns: {list(entity_columns.keys())}")
        for col, label in entity_columns.items():
            if col in df.columns:
                df[col] = df[col].fillna(label)
        return df

    def handle_structural_missing_dates(self, df: pd.DataFrame, target_column: str, flag_name: str) -> pd.DataFrame:
        """
        Engineers a boolean flag from structural missing dates (like active employees).
        """
        logger.info(f"Engineering structural missing flag for {target_column} as {flag_name}")
        if target_column in df.columns:
            df[flag_name] = df[target_column].isna()
        return df
