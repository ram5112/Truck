import configparser
import os
import pandas as pd
from sqlalchemy import create_engine, inspect
import sys
import hopsworks

# Define the parent directory path
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from src.components.data_cleaning import DataClean  # Import your existing DataClean class

# Define path to the config file
config_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'config', 'config.ini'))

# Check if the config file exists
if not os.path.isfile(config_file_path):
    raise FileNotFoundError(f"Configuration file not found: {config_file_path}")

class DataCleaningPipeline:
    def __init__(self):
        self.cleaning_obj = DataClean()
        self.engine = self.cleaning_obj.engine

    def get_all_tables(self):
        """Get all table names from the database."""
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def process_specific_to_datasets(self, df, table_name):
        """Process DataFrame based on the specific dataset."""
        df_cleaned = df  # Default assignment

        try:
            # Apply cleaning based on the table name
            if 'city_weather' in table_name:
                df = self.cleaning_obj.merge_h_d(df)
                columns_to_drop = ['date', 'hour', 'chanceofrain', 'chanceoffog', 'chanceofsnow', 'chanceofthunder']
                df_cleaned.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True, errors='ignore')
                df_cleaned = self.cleaning_obj.city_routes_ro(df)
                
                
                df_cleaned['datetime'] = pd.to_datetime(df_cleaned['datetime'], errors='coerce')

            elif 'drivers' in table_name:
                df = self.cleaning_obj.exp(df)
                df = self.cleaning_obj.fill_drivers(df)
                df_cleaned = self.cleaning_obj.drivers_ro(df)

            elif 'truck_schedule' in table_name:
                df['estimated_arrival'] = pd.to_datetime(df['estimated_arrival'], errors='coerce')
                df_cleaned = df  # Preserving df if no further cleaning is specified

            elif 'routes_weather' in table_name:
                columns_to_drop = ['chanceofrain', 'chanceoffog', 'chanceofsnow', 'chanceofthunder']
                df_cleaned.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True, errors='ignore')
                df_cleaned = self.cleaning_obj.city_routes_ro(df)
                

            elif 'traffic' in table_name:
                df = self.cleaning_obj.merge_h_d(df)
                df.drop(columns=['date', 'hour'], inplace=True, errors='ignore')
                if 'no_of_vehicles' in df.columns:
                    df['no_of_vehicles'] = df['no_of_vehicles'].fillna(df['no_of_vehicles'].mode()[0])
                df_cleaned = self.cleaning_obj.remove_outliers_iqr(df)

            elif 'trucks' in table_name:
                df.dropna(subset=['load_capacity_pounds', 'fuel_type'], inplace=True)
                df_cleaned = self.cleaning_obj.trucks_ro(df)

            elif 'routes' in table_name:
                df_cleaned = self.cleaning_obj.remove_outliers_iqr(df)

        except Exception as ex:
            print(f"Error processing {table_name}: {ex}")
            return df  # In case of error, return the original unprocessed DataFrame
        return df_cleaned

    def main(self):
        """Run the data cleaning pipeline."""
        try:
            tables = self.get_all_tables()
            for table_name in tables:
                if not table_name.endswith('_cleaned'):  # Skip already cleaned tables
                    print(f"Processing table: {table_name}")
                    
                    df = self.cleaning_obj.read_table(table_name)
                    
                    # Check if DataFrame is empty
                    if df.empty:
                        print(f"Skipping empty table: {table_name}")
                        continue
                    
                    df = self.cleaning_obj.drop_duplicates(df)
                    
                    df_cleaned = self.process_specific_to_datasets(df, table_name)
                    
                    df_cleaned = self.cleaning_obj.add_date(df_cleaned)  # Add event_date first
                    df_cleaned = self.cleaning_obj.add_index_column(df_cleaned)

                    # Save the cleaned DataFrame as a feature group in Hopsworks
                    feature_group = self.cleaning_obj.save_cleaned_data_hopsworks(df_cleaned, table_name)
                    if feature_group:
                        self.cleaning_obj.configure_and_compute_statistics(feature_group)

            print("All tables processed successfully.")

        except Exception as e:
            print(f"An error occurred during the pipeline execution: {e}")
            raise e


if __name__ == '__main__':
    try:
        print(">>>> DATA CLEANING <<<<")
        pipeline = DataCleaningPipeline()
        pipeline.main()
        print(">>>> STAGE COMPLETED <<<<")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise e
