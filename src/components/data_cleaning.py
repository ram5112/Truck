import configparser
import os
import pandas as pd
from sqlalchemy import create_engine, inspect
from datetime import datetime
import sys

# Define the parent directory path
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

class DataClean:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini'))

        # Create database connection
        self.engine = create_engine(f"postgresql://{self.config['POSTGRESQL']['user']}:{self.config['POSTGRESQL']['password']}@{self.config['POSTGRESQL']['host']}:{self.config['POSTGRESQL']['port']}/{self.config['POSTGRESQL']['dbname']}")

    def read_table(self, table_name):
        """
        Reads data from a given table in the database.
        """
        return pd.read_sql_table(table_name, self.engine)

    def save_cleaned_data(self, df, table_name):
        """
        Saves cleaned dataframe back to the database with '_cleaned' suffix.
        """
        cleaned_table_name = f"{table_name}_cleaned"
        df.to_sql(cleaned_table_name, self.engine, if_exists='replace', index=False)
        print(f"Cleaned data saved to table {cleaned_table_name}")

    def drop_duplicates(self, df):
        """
        Removes duplicates from the dataframe.
        """
        return df.drop_duplicates()

    def remove_outliers_iqr(self, df):
        """
        Removes outliers using IQR for numeric columns.
        """
        for column in df.select_dtypes(include=['float64', 'int64']).columns:
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
        return df

    def add_index_column(self, df):
        """
        Adds an index column to the dataframe.
        """
        df.reset_index(drop=False, inplace=True)
        return df

    def add_date(self, df):
        df['event_date'] = datetime.today().strftime('%Y-%m-%d')
        return df