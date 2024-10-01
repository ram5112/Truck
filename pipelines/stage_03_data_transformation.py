import configparser
import os
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, inspect
import sys
import hopsworks

parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from src.components.data_transformation import Transformation

config_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'config', 'config.ini'))

if not os.path.isfile(config_file_path):
    raise FileNotFoundError(f"Configuration file not found: {config_file_path}")

class DataTransformationPipeline:
    def __init__(self):
        self.transformation = Transformation()
        self.feature_group_names = [
            'city_weather_features', 'trucks_features', 'trucks_schedule_features',
            'routes_weather_features', 'routes_features', 'traffic_features', 'drivers_features'
        ]

    def main(self):
        print("Logging in to Hopsworks...")
        feature_dataframes = {}

        for feature_name in self.feature_group_names:
            feature_group = self.transformation.get_feature_groups(feature_name)
            df = self.transformation.read_feature_groups(feature_group)
            df = self.transformation.drop_event_date(df)
            feature_dataframes[feature_name] = df

        df_city_weather = feature_dataframes['city_weather_features']
        df_trucks = feature_dataframes['trucks_features']
        df_truck_schedule = feature_dataframes['trucks_schedule_features']
        df_routes_weather = feature_dataframes['routes_weather_features']
        df_routes = feature_dataframes['routes_features']
        df_traffic = feature_dataframes['traffic_features']
        df_drivers = feature_dataframes['drivers_features']

        df_city_weather = self.transformation.change_date_1(df_city_weather)
        df_traffic = self.transformation.change_date_1(df_traffic)
        df_truck_schedule = self.transformation.change_date_2(df_truck_schedule)

        # Drop duplicates
        df_city_weather.drop_duplicates(subset=['city_id', 'datetime'], inplace=True)
        df_routes_weather.drop_duplicates(subset=['route_id', 'date'], inplace=True)
        df_trucks.drop_duplicates(subset=['truck_id'], inplace=True)
        df_drivers.drop_duplicates(subset=['driver_id'], inplace=True)
        df_routes.drop_duplicates(subset=['route_id', 'destination_id', 'origin_id'], inplace=True)
        df_truck_schedule.drop_duplicates(subset=['truck_id', 'route_id', 'departure_date'], inplace=True)

        df_truck_schedule1 = df_truck_schedule.copy()
        df_truck_schedule1 = self.transformation.nearest_6H(df_truck_schedule1)
        df_truck_schedule1 = self.transformation.add_date(df_truck_schedule1)
        df_truck_schedule1 = df_truck_schedule1.explode('date')
        df_truck_schedule1 = df_truck_schedule1.drop_duplicates()

        df_routes_weather['date'] = pd.to_datetime(df_routes_weather['date'])
        df_truck_schedule1['date'] = pd.to_datetime(df_truck_schedule1['date'])

        print("df_truck_schedule1 dtypes:", df_truck_schedule1.dtypes)
        print("df_routes_weather dtypes:", df_routes_weather.dtypes)
        print("df_truck_schedule1 sample:", df_truck_schedule1[['route_id', 'date']].head())
        print("df_routes_weather sample:", df_routes_weather[['route_id', 'date']].head())

        df_merged = pd.merge(df_truck_schedule1, df_routes_weather, on=['route_id', 'date'], how='left')

        if df_merged is None:
            print("Merge operation failed. Please check the data types and values of the joining columns.")
            return

        df_merged.drop(columns=['date'], inplace=True)
        df_merged = df_merged.dropna(subset=['temp', 'wind_speed', 'description', 'humidity', 'pressure'])
        print(df_merged.shape)
        
        scheduled_weather = pd.DataFrame(df_merged)

        schedule_weather_grp = scheduled_weather.groupby(['truck_id', 'route_id'], as_index=False).agg(
            route_avg_temp=('temp', 'mean'),
            route_avg_wind_speed=('wind_speed', 'mean'),
            route_avg_humidity=('humidity', 'mean'),
            route_avg_pressure=('pressure', 'mean'),
            route_precip = ('precip','mean'),
            route_visibility = ('visibility','mean'),
            route_description=('description', self.transformation.custom_mode)
        )
        

        # schedule_weather_grp = schedule_weather_grp.rename(columns={'index_x': 'index'})
        schedule_weather_merge = df_truck_schedule.merge(schedule_weather_grp, on=['truck_id', 'route_id'], how='left')
        schedule_weather_merge = schedule_weather_merge.dropna(subset=['route_avg_temp', 'route_avg_wind_speed', 'route_avg_humidity', 'route_avg_pressure', 'route_description'])
        schedule_weather_merge = schedule_weather_merge.drop_duplicates()
        print(schedule_weather_merge.shape)

        nearest_hour_schedule_df = df_truck_schedule.copy()
        nearest_hour_schedule_df['estimated_arrival'] = pd.to_datetime(nearest_hour_schedule_df['estimated_arrival'], errors='coerce')
        nearest_hour_schedule_df['departure_date'] = pd.to_datetime(nearest_hour_schedule_df['departure_date'], errors='coerce')
        nearest_hour_schedule_df['estimated_arrival_nearest_hour'] = nearest_hour_schedule_df['estimated_arrival'].dt.round("H")
        nearest_hour_schedule_df['departure_date_nearest_hour'] = nearest_hour_schedule_df['departure_date'].dt.round("H")

        nearest_hour_schedule_route_df = pd.merge(nearest_hour_schedule_df, df_routes, on='route_id', how='left')
        nearest_hour_schedule_route_df = nearest_hour_schedule_route_df.dropna()
        nearest_hour_schedule_route_df = nearest_hour_schedule_route_df.drop_duplicates()
        print(nearest_hour_schedule_route_df.shape)

        origin_weather = df_city_weather.copy()
        destination_weather = df_city_weather.copy()

        origin_weather_merge = pd.merge(nearest_hour_schedule_route_df, origin_weather, left_on=['origin_id', 'departure_date_nearest_hour'], right_on=['city_id', 'datetime'], how='left')
        origin_weather_merge = origin_weather_merge.dropna()
        origin_weather_merge.rename(columns={
            'temp': 'origin_temp',
            'wind_speed': 'origin_wind_speed',
            'description': 'origin_description',
            'humidity': 'origin_humidity',
            'pressure': 'origin_pressure',
            'precip' : 'origin_precip',
            'visibility' : 'origin_visibility'
        }, inplace=True)
        origin_weather_merge = origin_weather_merge.drop(columns='datetime')
        print(origin_weather_merge.shape)

        final_merge = pd.merge(origin_weather_merge, destination_weather, left_on=['destination_id', 'estimated_arrival_nearest_hour'], right_on=['city_id', 'datetime'], how='left', suffixes=('_origin', '_destination'))
        final_merge = final_merge.dropna()
        final_merge.rename(columns={
            'temp': 'dest_temp',
            'wind_speed': 'dest_wind_speed',
            'description': 'dest_description',
            'humidity': 'dest_humidity',
            'pressure': 'dest_pressure',
            'precip': 'dest_precip',
            'visibility':'dest_visibility'
        }, inplace=True)
        final_merge = final_merge.drop(columns=['datetime', 'city_id_origin', 'city_id_destination'])
        final_merge = final_merge.drop_duplicates()
        print(final_merge.shape)

        schedule_data = df_truck_schedule.copy()
        schedule_data['departure_date'] = pd.to_datetime(schedule_data['departure_date'])
        schedule_data['estimated_arrival'] = pd.to_datetime(schedule_data['estimated_arrival'])
        schedule_data['departure_date'] = schedule_data['departure_date'].dt.round('H')
        schedule_data['estimated_arrival'] = schedule_data['estimated_arrival'].dt.round('H')

        hourly_exploded_scheduled_df = (nearest_hour_schedule_df.assign(custom_date=[pd.date_range(start, end, freq='H')
                                                                                     for start, end
                                                                                     in zip(nearest_hour_schedule_df['departure_date'], nearest_hour_schedule_df['estimated_arrival'])])
                                        .explode('custom_date', ignore_index=True))

        df_traffic['datetime'] = pd.to_datetime(df_traffic['datetime'])
        hourly_exploded_scheduled_df = hourly_exploded_scheduled_df.drop_duplicates()

        scheduled_traffic = hourly_exploded_scheduled_df.merge(df_traffic, left_on=['route_id', 'custom_date'], right_on=['route_id', 'datetime'], how='left')
        scheduled_traffic = scheduled_traffic.dropna()
        scheduled_traffic = scheduled_traffic.drop_duplicates()

        scheduled_route_traffic = scheduled_traffic.groupby(['truck_id', 'route_id'], as_index=False).agg(
            avg_no_of_vehicles=('no_of_vehicles', 'mean'),
            accident=('accident', lambda x: self.transformation.custom_agg(x))
        )

        origin_destination_weather_traffic_merge = final_merge.merge(scheduled_route_traffic, on=['truck_id', 'route_id'], how='left')
        origin_destination_weather_traffic_merge = origin_destination_weather_traffic_merge.dropna()
        origin_destination_weather_traffic_merge = origin_destination_weather_traffic_merge.drop_duplicates()

        # Convert 'departure_date' to datetime in both DataFrames
        schedule_weather_merge['departure_date'] = pd.to_datetime(schedule_weather_merge['departure_date'])
        origin_destination_weather_traffic_merge['departure_date'] = pd.to_datetime(origin_destination_weather_traffic_merge['departure_date'])

        merged_data_weather_traffic = pd.merge(schedule_weather_merge, origin_destination_weather_traffic_merge, on=['truck_id', 'route_id', 'departure_date', 'estimated_arrival', 'delay'], how='left')
        merged_data_weather_traffic = merged_data_weather_traffic.dropna()
        merged_data_weather_traffic = merged_data_weather_traffic.drop_duplicates()

        merged_data_weather_traffic_trucks = pd.merge(merged_data_weather_traffic, df_trucks, on='truck_id', how='left')
        merged_data_weather_traffic_trucks = merged_data_weather_traffic_trucks.dropna()
        
        final_merge_dataset = pd.merge(merged_data_weather_traffic_trucks,
                                       df_drivers, left_on='truck_id', right_on='vehicle_no', how='left')
        print(final_merge_dataset.shape)
        final_merge_dataset['is_midnight'] = final_merge_dataset.apply(
                lambda row: self.transformation.has_midnight(row['departure_date'], row['estimated_arrival']),
                axis=1
                )
        final_merge_dataset = final_merge_dataset.dropna()
        final_merge_dataset = final_merge_dataset.drop_duplicates()
        final_merge_dataset['unique_id'] = range(1, len(final_merge_dataset) + 1)
        
        print(final_merge_dataset)
        print(final_merge_dataset.columns)
        print(final_merge_dataset.shape)

if __name__ == "__main__":
    print(">>>Transformation Starts<<<<")
    pipeline = DataTransformationPipeline()
    pipeline.main()
    print(">>>Transformation Done<<<<")