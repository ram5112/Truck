import configparser
import os
import pandas as pd
import sys

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
        # Use the paths from the DataClean object
        self.raw_data_dir = self.cleaning_obj.raw_data_dir
        self.cleaned_data_dir = self.cleaning_obj.cleaned_data_dir

    def process_specific_to_datasets(self, df, file_name):
        df_cleaned = df  # Default assignment to prevent undefined variable issues

        if 'city_weather' in file_name:
            df['date'] = pd.to_datetime(df['date'])
            df['hour'] = df['hour'].apply(lambda x: f"{x // 100:02}:{x % 100:02}:00")
            df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['hour'])
            df = df.drop(columns=['date', 'hour'])
            df = df.drop(columns=['chanceofrain', 'chanceoffog', 'chanceofsnow', 'chanceofthunder', 'precip', 'visibility'])
            df_cleaned = self.cleaning_obj.remove_outliers_iqr(df)

        elif 'drivers' in file_name:
            df=df.drop(columns=['gender'])
            mean_experience = df['experience'][df['experience'] >= 0].mean()
            df['experience'] = df['experience'].apply(lambda x: mean_experience if x < 0 else x)

            for column in ['driving_style']:
                mode_value = df[column].mode()[0]
                df[column] = df[column].fillna(mode_value)

            for column in ['age', 'experience']:
                Q1 = df[column].quantile(0.25)  # First quartile (25th percentile)
                Q3 = df[column].quantile(0.75)  # Third quartile (75th percentile)
                IQR = Q3 - Q1  # Interquartile range

                # Define the bounds for outliers
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR

                # Remove outliers from the column
                df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

            df_cleaned = df  # Update df_cleaned after outlier removal

        elif 'truck_schedule' in file_name:
            df['estimated_arrival'] = pd.to_datetime(df['estimated_arrival']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        elif 'routes_weather' in file_name:
            df = df.drop(columns=['chanceofrain', 'chanceoffog', 'chanceofsnow', 'chanceofthunder', 'precip', 'visibility'])
            df_cleaned = self.cleaning_obj.remove_outliers_iqr(df)

        elif 'traffic' in file_name:
            df['date'] = pd.to_datetime(df['date'])
            df['hour'] = df['hour'].apply(lambda x: f"{x // 100:02}:{x % 100:02}:00")
            df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['hour'])
            df = df.drop(columns=['date', 'hour'])
            mode_value = df['no_of_vehicles'].mode()[0]
            df['no_of_vehicles'] = df['no_of_vehicles'].fillna(mode_value)
            df_cleaned = self.cleaning_obj.remove_outliers_iqr(df)

        elif 'trucks' in file_name:
            df = df.dropna(subset=['load_capacity_pounds', 'fuel_type'])

            for column in ['truck_age']:
                Q1 = df[column].quantile(0.25)  # First quartile (25th percentile)
                Q3 = df[column].quantile(0.75)  # Third quartile (75th percentile)
                IQR = Q3 - Q1  # Interquartile range

                # Define the bounds for outliers
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR

                # Remove outliers from the column
                df_cleaned = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

        # Return the cleaned DataFrame
        return df_cleaned

    def main(self):
        try:
            for filename in os.listdir(self.raw_data_dir):
                if filename.endswith('.csv'):
                    print(f"Processing file: {filename}")
                    file_path = os.path.join(self.raw_data_dir, filename)
                    df = self.cleaning_obj.read_data(file_path)
                    df = self.cleaning_obj.drop_duplicates(df)
                    df_cleaned = self.process_specific_to_datasets(df, filename)
                    df_cleaned = self.cleaning_obj.add_date(df_cleaned)
                    df_cleaned = self.cleaning_obj.add_index_column(df_cleaned)
                    # Save the cleaned DataFrame
                    cleaned_filename = f"df_{filename.split('.')[0]}_cleaned.csv"
                    output_file_path = os.path.join(self.cleaned_data_dir, cleaned_filename)
                    df_cleaned.to_csv(output_file_path, index=False)
                    print(f"Saved cleaned file: {output_file_path}")

        except Exception as e:
            print(f"An error occurred: {e}")
            raise e

# Ensure this block is outside the class definition
if __name__ == '__main__':
    try:
        print(">>>> DATA CLEANING <<<<")
        obj = DataCleaningPipeline()
        obj.main()
        print(">>>> STAGE COMPLETED <<<<")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise e
