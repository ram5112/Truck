import configparser
import os
import pandas as pd
from sqlalchemy import create_engine, inspect
from datetime import datetime
import sys
import hopsworks
from hsfs.feature import Feature

# Define the parent directory path
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

class Transformation:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini'))

        # Login to Hopsworks
        self.project = hopsworks.login(
            project=self.config['API']['project_name'],
            api_key_value=self.config['API']['api_key']
        )
        # Initialize the feature store
        self.feature_store = self.project.get_feature_store()

    def get_feature_groups(self, name):
        feature_group = self.feature_store.get_feature_group(name)
        return feature_group

    def read_feature_groups(self, feature_group):
        df = feature_group.read()
        return df

    def drop_event_date(self, df):
        if 'event_date' in df.columns:
            df = df.drop(columns=['index','event_date'])
        return df

    def nearest_H(self, df):
        df['estimated_arrival_nearest_hour'] = df['estimated_arrival'].dt.round("H")
        df['departure_date_nearest_hour'] = df['departure_date'].dt.round("H")
    
    
    def change_date_1(self, df):
        df['datetime'] = pd.to_datetime(df['datetime'].astype(str).str.split('+').str[0])
        return df
    
    def change_date_2(self, df):
        df['estimated_arrival'] = pd.to_datetime(df['estimated_arrival'].astype(str).str.split('+').str[0].str.split('.').str[0])
        return df

    def nearest_6H(self, df):
        df['estimated_arrival'] = pd.to_datetime(df['estimated_arrival']).dt.ceil('6H')
        df['departure_date'] = pd.to_datetime(df['departure_date']).dt.floor('6H')
        return df
    
    def add_date(self, df):
        df['date'] = [pd.date_range(start=row['departure_date'], end=row['estimated_arrival'], freq='6H')
                      for _, row in df.iterrows()]
        return df
    
    @staticmethod
    def custom_mode(x):
        return x.mode().iloc[0] if not x.empty else None
    
    def custom_agg(values, *args, **kwargs):
        if isinstance(values, pd.Series):  # Check if 'values' is a Series
            if any(values == 1):  # Apply the comparison on the Series
                return 1
            else:
                return 0
        else:
            return 1 if values == 1 else 0  


    

    def has_midnight(self, start, end):
        return int(start.date() != end.date())

        
    @staticmethod
    def merge_dataframes(df_left, df_right, on, how='left'):
        try:
            df_merged = pd.merge(df_left, df_right, on=on, how=how)
            return df_merged
        except KeyError as e:
            print(f"Merge failed: Column not found. {e}")
        except Exception as e:
            print(f"An error occurred during merge: {e}")
