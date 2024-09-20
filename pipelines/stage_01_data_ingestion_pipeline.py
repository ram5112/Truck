import os
import sys
import configparser
from io import StringIO 

parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from src.components.data_ingestion import PostgreSQLIngestion

class DataIngestionPipeline:
    def __init__(self):
        self.config = self.read_config()

    @staticmethod
    def read_config(config_file=None):
        if config_file is None:
            config_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'config', 'config.ini'))

        config = configparser.ConfigParser()
        if not os.path.isfile(config_file):
            print(f"Config file not found: {config_file}")
            return None

        config.read(config_file)
        return config

    def main(self):
        print("Starting data ingestion...")

        try:
            # Reading database configuration
            db_config = {
                'host': self.config['POSTGRESQL']['host'].strip(),
                'port': int(self.config['POSTGRESQL']['port'].strip()),
                'dbname': self.config['POSTGRESQL']['dbname'].strip(),
                'user': self.config['POSTGRESQL']['user'].strip(),
                'password': self.config['POSTGRESQL']['password'].strip(),
                'source_url': self.config['DATA']['source_url'].strip()
            }

            # Initialize PostgreSQL ingestion
            ingestion = PostgreSQLIngestion(db_config)
            ingestion.connect()

            # Fetch raw CSV files
            csv_files = ingestion.fetch_github_raw_files()

            # Upload each CSV to PostgreSQL
            for file_url in csv_files:
                table_name = file_url.split('/')[-1].replace('.csv', '')
                ingestion.upload_to_postgres(file_url, table_name)

        except Exception as e:
            print(f"An error occurred: {e}")
            raise e

        finally:
            if 'ingestion' in locals():
                ingestion.close()

if __name__ == '__main__':
    try:
        STAGE_NAME = "Data Ingestion"
        print(f">>>>>> Stage {STAGE_NAME} started <<<<<<")
        pipeline = DataIngestionPipeline()
        pipeline.main()
        print(f">>>>>> Stage {STAGE_NAME} completed <<<<<<")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise e