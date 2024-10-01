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

class DataClean:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini'))

        # Use the correct login method based on the latest Hopsworks documentation
        self.project = hopsworks.login(
            project=self.config['API']['project_name'],
            api_key_value=self.config['API']['api_key']
        )
        
        # Create database connection
        self.engine = create_engine(
            f"postgresql://{self.config['POSTGRESQL']['user']}:{self.config['POSTGRESQL']['password']}@{self.config['POSTGRESQL']['host']}:{self.config['POSTGRESQL']['port']}/{self.config['POSTGRESQL']['dbname']}"
        )
        
        # Initialize the feature store
        self.feature_store = self.project.get_feature_store()
        
    def read_table(self, table_name):
        """
        Reads data from a given table in the database.
        """
        return pd.read_sql_table(table_name, self.engine)

    def save_cleaned_data_hopsworks(self, df, table_name):
        feature_group_mapping = {
            'city_weather': 'city_weather_features',
            'drivers_table': 'drivers_features',
            'routes_table': 'routes_features',
            'routes_weather': 'routes_weather_features',
            'traffic_table': 'traffic_features',
            'trucks_table': 'trucks_features',
            'truck_schedule_table': 'trucks_schedule_features'
        }
        
        feature_group_name = feature_group_mapping.get(table_name, f"{table_name}_features")
        
        # Ensure correct data types
        df['index'] = df['index'].astype('int32')
        df['event_date'] = pd.to_datetime(df['event_date'])

        # Convert 'double' columns to 'float32'
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = df[col].astype('float32')

        # Convert 'int64' columns to 'int32'
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = df[col].astype('int32')

        # Convert 'datetime64[ns]' to 'datetime64[ms]'
        for col in df.select_dtypes(include=['datetime64']).columns:
            df[col] = df[col].astype('datetime64[ms]')

        try:
            feature_group = self.feature_store.get_feature_group(feature_group_name, version=1)
            print(f"Feature group '{feature_group_name}' already exists. Skipping insertion.")
            return  # Skip insertion and return from the method
        except:
            print(f"Creating new feature group '{feature_group_name}'.")
            feature_group = self.feature_store.create_feature_group(
                name=feature_group_name,
                version=1,
                description=f"Cleaned data for {table_name}",
                primary_key=['index'],
                event_time='event_date'
            )
            
            features = []
            for column, dtype in df.dtypes.items():
                if column == 'event_date':
                    features.append(Feature(column, type="timestamp"))
                elif column == 'index':
                    features.append(Feature(column, type="int"))
                elif dtype == 'object':
                    features.append(Feature(column, type="string"))
                elif dtype in ['float64', 'float32']:
                    features.append(Feature(column, type="float"))
                elif dtype in ['int64', 'int32']:
                    features.append(Feature(column, type="int"))
                elif dtype == 'datetime64[ms]':
                    features.append(Feature(column, type="timestamp"))
                else:
                    features.append(Feature(column, type=dtype.name))
            
            feature_group.features = features

            # Insert data only for newly created feature groups
            feature_group.insert(df, write_options={"wait_for_job": False})
            print(f"Data saved to new feature group '{feature_group_name}'.")
        return feature_group

    def configure_and_compute_statistics(self, feature_group):
        """Configure and compute statistics for the feature group."""
        try:
            # Check if the feature group has any commits (i.e., data)
            commits = feature_group.get_commits()
            if not commits:
                print(f"No commits found for the feature group: {feature_group.name}. Skipping statistics computation.")
                return
            
            # Proceed to compute statistics if commits exist
            print(f"Computing statistics for the feature group: {feature_group.name}")
            feature_group.compute_statistics()
            print(f"Statistics computed successfully for the feature group: {feature_group.name}")
        
        except Exception as e:
            print(f"An error occurred while computing statistics for the feature group: {feature_group.name}. Error: {e}")
        
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
        df['index'] = df['index'].astype('int64')  # Ensure index is int64
        return df

    def add_date(self, df):
        df['event_date'] = datetime.today().strftime('%Y-%m-%d')
        return df
    
    def merge_h_d(self, df):
        df['date'] = pd.to_datetime(df['date'])
        df['hour'] = df['hour'].apply(lambda x: f"{x // 100:02}:{x % 100:02}:00")
        df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['hour'])
        return df
    
    def exp(self, df):
        mean_experience = df['experience'][df['experience'] >= 0].mean()
        df['experience'] = df['experience'].apply(lambda x: mean_experience if x < 0 else x)
        return df
    
    def city_routes_ro(self,df):
        for column in ['temp','wind_speed','humidity','pressure']:
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
        return df
    
    def drivers_ro(self, df):
        for column in ['age', 'experience']:
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
        return df
    
    def trucks_ro(self, df):
        for column in ['truck_age']:
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
        return df
    
    def fill_drivers(self, df):
        for column in ['driving_style']:
            mode_value = df[column].mode()[0]
            df[column] = df[column].fillna(mode_value)
        return df
    
    def fill_traffic(self, df):
        mode_value = df['no_of_vehicles'].mode()[0]
        df['no_of_vehicles'] = df['no_of_vehicles'].fillna(mode_value)
