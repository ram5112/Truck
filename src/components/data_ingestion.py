import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO 
import configparser

class PostgreSQLIngestion:
    def __init__(self, db_config):
        self.db_config = db_config
        self.engine = None

    def connect(self):
        try:
            self.engine = create_engine(
                f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}")
            print("Database connection successful.")
        except Exception as e:
            print(f"Error connecting to PostgreSQL database: {e}")
            raise

    def fetch_github_raw_files(self):
        """
        Fetch the raw CSV file URLs from the GitHub repository.
        """
        github_api_url = self.db_config['source_url']
        response = requests.get(github_api_url)
        if response.status_code == 200:
            files = response.json()
            return [file['download_url'] for file in files if file['name'].endswith('.csv')]
        else:
            raise Exception(f"Failed to fetch files from GitHub. Status code: {response.status_code}")

    def upload_to_postgres(self, file_url, table_name):
        try:
            print(f"Uploading file {file_url} to PostgreSQL table {table_name}...")
            response = requests.get(file_url, verify=False)  # Verify can be set to False for testing
            response.raise_for_status()  # Raise an error for bad responses
            df = pd.read_csv(StringIO(response.text))  # Use StringIO from io module
            print(f"Data from {file_url}:\n{df.head()}")  # Debugging: Check if data is loaded correctly
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            print(f"Data from {file_url} uploaded to table {table_name}")
        except Exception as e:
            print(f"Error uploading data from {file_url} to table {table_name}: {e}")



    def close(self):
        if self.engine:
            self.engine.dispose()
        print("Database connection closed.")