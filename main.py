import os
import pandas as pd
from src.logger import setup_logger
from src.extractor import DataLoader
from src.transformer import DataTransformer
from src.nlp_processor import NLPProcessor
from src.load import DatabaseLoader

logger = setup_logger("MainPipeline")

class ETLPipeline:
    """Orchestrates the Extraction, Transformation, and Load pipeline."""
    
    def __init__(self, raw_dir="data/raw", processed_dir="data/processed"):
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        
        # Ensure processed directory exists
        os.makedirs(self.processed_dir, exist_ok=True)
        
        # Modules
        self.loader = DataLoader(self.raw_dir)
        self.transformer = DataTransformer()
        self.nlp = NLPProcessor()
        self.db_loader = DatabaseLoader()

    def process_fuel_purchases(self):
        """Pipeline for fuel_purchases.csv"""
        logger.info("--- Processing Fuel Purchases ---")
        df = self.loader.load_csv("fuel_purchases.csv")
        
        # Clean timestamps
        df = self.transformer.standardize_timestamps(df, time_columns=['purchase_date'])
        
        # Clean missing entity IDs (Ghost Dimensions)
        df = self.transformer.handle_missing_entities(df, {
            'driver_id': 'UNKNOWN_DRIVER',
            'truck_id': 'UNKNOWN_TRUCK'
        })
        
        # Cast Types
        df = self.transformer.cast_data_types(df, {'total_cost': 'float64', 'price_per_gallon': 'float64', 'gallons': 'float64', 'truck_id': 'str'})
        
        # Save output
        out_path = os.path.join(self.processed_dir, "clean_fuel_purchases.csv")
        df.to_csv(out_path, index=False)
        self.db_loader.load_table(df, 'fuel_purchases')
        
    def process_delivery_events(self):
        """Pipeline for delivery_events.csv"""
        logger.info("--- Processing Delivery Events ---")
        df = self.loader.load_csv("delivery_events.csv")
        
        # Clean expected timestamps
        time_cols = ['scheduled_datetime', 'actual_datetime']
        df = self.transformer.standardize_timestamps(df, time_columns=time_cols)
        
        # Flag telematics drops (missing actual arrival times)
        df = self.transformer.flag_missing_telematics(df, target_column='actual_datetime')
        
        # Save output
        out_path = os.path.join(self.processed_dir, "clean_delivery_events.csv")
        df.to_csv(out_path, index=False)
        self.db_loader.load_table(df, 'delivery_events')
        
    def process_maintenance_logs(self):
        """Pipeline for maintenance_records.csv with NLP"""
        logger.info("--- Processing Maintenance Logs ---")
        df = self.loader.load_csv("maintenance_records.csv")
        
        # Parse text into categories contextually
        df = self.nlp.process_dataframe(df, text_column='service_description', new_category_col='Delay_Reason', context='maintenance')
        
        # Types
        df = self.transformer.cast_data_types(df, {'total_cost': 'float64', 'labor_cost': 'float64', 'parts_cost': 'float64', 'truck_id': 'str'})
        
        # Quarrantine check (e.g. negative costs)
        errors_mask = df['total_cost'] < 0
        if errors_mask.any():
            error_df = df[errors_mask]
            error_df.to_csv(os.path.join(self.processed_dir, "error_quarantine_maintenance.csv"), index=False)
            logger.warning(f"Quarantined {len(error_df)} rows from maintenance logs due to negative costs.")
            # Drop errors from clean stream
            df = df[~errors_mask]
            
        # Save output
        out_path = os.path.join(self.processed_dir, "clean_maintenance_records.csv")
        df.to_csv(out_path, index=False)
        self.db_loader.load_table(df, 'maintenance_records')
        
    def process_safety_incidents(self):
        """Pipeline for safety_incidents.csv with NLP"""
        logger.info("--- Processing Safety Incidents ---")
        df = self.loader.load_csv("safety_incidents.csv")
        
        # Clean timestamps
        df = self.transformer.standardize_timestamps(df, time_columns=['incident_date'])
        
        # Clean missing entity IDs (Ghost Dimensions)
        df = self.transformer.handle_missing_entities(df, {
            'driver_id': 'UNKNOWN_DRIVER',
            'truck_id': 'UNKNOWN_TRUCK'
        })
        
        # Parse text into categorized incident types contextually
        df = self.nlp.process_dataframe(df, text_column='description', new_category_col='Incident_Category', context='safety')
        
        # Types
        df = self.transformer.cast_data_types(df, {'vehicle_damage_cost': 'float64', 'cargo_damage_cost': 'float64', 'truck_id': 'str'})
        
        # Save output
        out_path = os.path.join(self.processed_dir, "clean_safety_incidents.csv")
        df.to_csv(out_path, index=False)
        self.db_loader.load_table(df, 'safety_incidents')

    def process_trips(self):
        """Pipeline for trips.csv"""
        logger.info("--- Processing Trips ---")
        df = self.loader.load_csv("trips.csv")
        
        # Clean expected timestamps
        df = self.transformer.standardize_timestamps(df, time_columns=['dispatch_date'])
        
        # Clean missing entity IDs (Ghost Dimensions)
        df = self.transformer.handle_missing_entities(df, {
            'driver_id': 'UNKNOWN_DRIVER',
            'truck_id': 'UNKNOWN_TRUCK',
            'trailer_id': 'UNKNOWN_TRAILER'
        })
        
        # Cast Types
        df = self.transformer.cast_data_types(df, {'actual_distance_miles': 'float64', 'actual_duration_hours': 'float64', 'fuel_gallons_used': 'float64', 'average_mpg': 'float64', 'idle_time_hours': 'float64', 'driver_id': 'str', 'truck_id': 'str', 'trailer_id': 'str'})
        
        # Save output
        out_path = os.path.join(self.processed_dir, "clean_trips.csv")
        df.to_csv(out_path, index=False)
        self.db_loader.load_table(df, 'trips')
        
    def process_drivers(self):
        """Pipeline for drivers.csv"""
        logger.info("--- Processing Drivers ---")
        df = self.loader.load_csv("drivers.csv")
        
        # Feature Engineering: Structural Missing Data
        df = self.transformer.handle_structural_missing_dates(df, target_column='termination_date', flag_name='is_active_driver')
        
        # Clean expected timestamps
        df = self.transformer.standardize_timestamps(df, time_columns=['hire_date', 'termination_date', 'date_of_birth'])
        
        # Save output
        out_path = os.path.join(self.processed_dir, "clean_drivers.csv")
        df.to_csv(out_path, index=False)
        self.db_loader.load_table(df, 'drivers')

    def process_other_files(self):
        """Loads all other raw files directly into the database without transformation."""
        logger.info("--- Processing Remaining Raw Files ---")
        specific_files = [
            "fuel_purchases.csv", "delivery_events.csv", "maintenance_records.csv",
            "safety_incidents.csv", "trips.csv", "drivers.csv"
        ]
        
        all_files = [f for f in os.listdir(self.raw_dir) if f.endswith('.csv')]
        other_files = [f for f in all_files if f not in specific_files]
        
        for file in other_files:
            logger.info(f"Loading raw file: {file}")
            df = self.loader.load_csv(file)
            table_name = file.replace('.csv', '')
            
            # Save copy to processed dir for consistency
            out_path = os.path.join(self.processed_dir, file)
            df.to_csv(out_path, index=False)
            
            # Load into DB
            self.db_loader.load_table(df, table_name)

    def run_all(self):
        """Executes all ETL specific pipelines sequentially."""
        logger.info("Starting logistics data pipeline...")
        try:
            self.process_drivers()
            self.process_trips()
            self.process_fuel_purchases()
            self.process_delivery_events()
            self.process_maintenance_logs()
            self.process_safety_incidents()
            self.process_other_files()
            logger.info("Pipeline completed successfully.")
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run_all()
