import os
import sys
import configparser

parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from src.components.data_ingestion import PostgreSQLIngestion

class DataIngestionPipeline:
    def __init__(self):
        self.config = self.read_config()
        if self.config is None:
            raise ValueError("Failed to read config file.")

    @staticmethod
    def read_config(config_file=None):
        if config_file is None:
            config_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'config', 'config.ini'))
    
        print(f"Attempting to read config file at: {config_file}")
        config = configparser.ConfigParser()
        if not os.path.isfile(config_file):
            print(f"Config file not found: {config_file}")
            return None
        config.read(config_file)
        print("Config sections:", config.sections())
        return config

    def main(self):
        print("Current working directory:", os.getcwd())

        try:
            db_config = {
                'host': self.config['POSTGRESQL']['host'].strip(),
                'port': int(self.config['POSTGRESQL']['port'].strip()),
                'dbname': self.config['POSTGRESQL']['dbname'].strip(),
                'user': self.config['POSTGRESQL']['user'].strip(),
                'password': self.config['POSTGRESQL']['password'].strip()
            }

            source_urls = {
                'city_weather': self.config['DATA']['city_weather_url'].strip(),
                'drivers': self.config['DATA']['drivers_url'].strip(),
                'routes': self.config['DATA']['routes_url'].strip(),
                'routes_weather': self.config['DATA']['routes_weather_url'].strip(),
                'traffic': self.config['DATA']['traffic_url'].strip(),
                'truck_schedule': self.config['DATA']['truck_schedule_url'].strip(),
                'trucks': self.config['DATA']['trucks_url'].strip()
            }

            raw_data_dir = self.config['DATA']['raw_data'].strip()
            git_download_dir = self.config['DATA']['git_download'].strip()

            os.makedirs(git_download_dir, exist_ok=True)
            os.makedirs(raw_data_dir, exist_ok=True)

            ingestion = PostgreSQLIngestion(db_config, source_urls, git_download_dir)
            ingestion.connect()

            for table_name, url in source_urls.items():
                print(f"Processing file for table: {table_name}")
                file_name = f"{table_name}.csv"
                file_path = ingestion.download_csv(url, file_name)

                if file_path:
                    ingestion.upload_to_postgres(file_path, table_name)

            tables = ingestion.get_table_names()

            if tables:
                print("Tables found:")
                print(tables)

                for table in tables:
                    print(f"Fetching data from table: {table}")
                    data = ingestion.fetch_data(table)

                    if data is not None:
                        file_path = os.path.join(raw_data_dir, f'{table}.csv')
                        data.to_csv(file_path, index=False)
                        print(f"Data from table {table} saved to {file_path}")
                    else:
                        print(f"No data fetched from table {table}")

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
