import configparser
config = configparser.RawConfigParser()
import os.path as path
from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import re
import sys
import os

current_file_path = os.path.abspath(__file__)

# Get the root directory of the project (two levels up from the current file)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file_path)))

# Add the root directory to sys.path
sys.path.append(project_root)

import os
import configparser
import pandas as pd

class DataClean:
    def __init__(self):
        self.config = configparser.RawConfigParser()
        self.config.read(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini'))

        self.raw_data_dir = self.config.get('DATA', 'raw_data')
        self.cleaned_data_dir = self.config.get('DATA', 'cleaned_data')
        
        if not os.path.exists(self.cleaned_data_dir):
            os.makedirs(self.cleaned_data_dir)

    def read_data(self, filepath):
        """
        Reads data from a given CSV file.
        """
        return pd.read_csv(filepath)

    def drop_duplicates(self, df):
        """
        Removes duplicates from the dataframe.
        """
        return df.drop_duplicates()
    
    def unique_values(df):
      for column in df.columns:
        unique_values = df[column].unique()
        print(f"Unique values in column '{column}':")
        print(unique_values)
        print("\n")
    
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

    def clean_data(self, df):
        """
        Apply all cleaning steps on the dataframe.
        """
        df = self.drop_duplicates(df)
        df = self.remove_outliers_iqr(df)
        return df

    def save_cleaned_data(self, df, filename):
        """
        Saves cleaned dataframe to the cleaned directory.
        """
        output_path = os.path.join(self.cleaned_data_dir, filename)
        df.to_csv(output_path, index=False)
        print(f"Cleaned data saved to {output_path}")

    def add_index_column(self, df):
        """
        Adds an index column to the dataframe.
        """
        df.reset_index(drop=False, inplace=True)
        return df
    
    def process_all_files(self):
        """
        Iterate over all raw data files, clean them, and save to the cleaned directory.
        """
        for filename in os.listdir(self.raw_data_dir):
            if filename.endswith('.csv'):
                file_path = os.path.join(self.raw_data_dir, filename)
                print(f"Processing file: {file_path}")

                # Read raw data
                df = self.read_data(file_path)
                
                # Clean data
                cleaned_df = self.clean_data(df)
                
                # Save cleaned data
                cleaned_filename = f"df_{filename.replace('.csv', '_cleaned.csv')}"
                self.save_cleaned_data(cleaned_df, cleaned_filename)

  
    def add_date(self,df):
        df['event_date'] = datetime.today().strftime('%Y-%m-%d')
        return df