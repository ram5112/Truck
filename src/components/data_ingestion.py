import os
import requests
import pandas as pd
from sqlalchemy import create_engine, inspect

class PostgreSQLIngestion:
    def __init__(self, db_config, source_urls, git_download_path):
        self.db_config = db_config
        self.source_urls = source_urls
        self.git_download_path = git_download_path
        self.engine = None

    def connect(self):
        try:
            self.engine = create_engine(f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}")
            print("Database connection successful.")
        except Exception as e:
            print(f"Error connecting to PostgreSQL database: {e}")
            raise

    def download_csv(self, url, file_name):
        try:
            response = requests.get(url)
            response.raise_for_status()
            file_path = os.path.join(self.git_download_path, file_name)
            with open(file_path, 'wb') as f:
                f.write(response.content)
            print(f"File {file_name} downloaded to {file_path}")
            return file_path
        except requests.exceptions.RequestException as e:
            print(f"Error downloading CSV file {file_name}: {e}")
            return None

    def upload_to_postgres(self, file_path, table_name):
        try:
            df = pd.read_csv(file_path)
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            print(f"Data uploaded to table {table_name}")
        except Exception as e:
            print(f"Error uploading data to table {table_name}: {e}")

    def get_table_names(self):
        try:
            inspector = inspect(self.engine)
            return inspector.get_table_names()
        except Exception as e:
            print(f"Error fetching table names: {e}")
            return []

    def fetch_data(self, table_name):
        try:
            query = f"SELECT * FROM {table_name}"
            return pd.read_sql_query(query, self.engine)
        except Exception as e:
            print(f"Error fetching data from table {table_name}: {e}")
            return None

    def close(self):
        if self.engine:
            self.engine.dispose()
        print("Database connection closed.")
