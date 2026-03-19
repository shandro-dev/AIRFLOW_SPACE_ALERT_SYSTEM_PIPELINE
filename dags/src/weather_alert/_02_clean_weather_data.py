# DATA CLEANING

import pandas as pd
import random
from datetime import datetime




def clean_weather_data(data):
#rounding off all float cols

    df=pd.DataFrame(data)

    df = df.round({
        'temperature_celcius': 2,
        'humidity_%': 0,
        'dew_temperature_celcius': 2,
        'feels_like_temperature_celcius': 2,
        'wind_speed_kmph':2,
        'precipitation_%': 0,
        'precipitation_occured_mm': 2,
        'rain_mm': 2,
        'showers_mm': 2,
        'snowfall_mm': 2,
        'snow_depth_mm': 2,
        'mean_sea_level_pressure_hpa': 2,
        'surface_pressure_hpa': 1,
        'cloud_cover_%': 0,
        'visibility_m': 2,
        'evapotranspiration_mm': 3,
        'et0_fao_evapotranspiration_mm': 3,
        'vapour_pressure_deficit_kpa': 4
    })


    #missing values check
    critical_columns = [
        "temperature_celcius", "humidity_%", "dew_temperature_celcius", "feels_like_temperature_celcius",
        "wind_speed_kmph", "precipitation_%", "precipitation_occured_mm", "rain_mm", "showers_mm", "snowfall_mm",
        "snow_depth_mm", "weather_code", "mean_sea_level_pressure_hpa", "surface_pressure_hpa", "cloud_cover_%",
        "visibility_m", "evapotranspiration_mm", "et0_fao_evapotranspiration_mm", "vapour_pressure_deficit_kpa"
    ]
    df['is_missing_data'] = df[critical_columns].isnull().any(axis=1)


    #outliers check
    thresholds = {
        "temperature_celcius": (-90, 60),
        "humidity_%": (0, 100),
        "dew_temperature_celcius": (-100, 60),
        "feels_like_temperature_celcius": (-100, 70),
        "wind_speed_kmph": (0, 300),
        "precipitation_%": (0, 100),
        "precipitation_occured_mm": (0, 500),
        "rain_mm": (0, 500),
        "showers_mm": (0, 500),
        "snowfall_mm": (0, 500),
        "snow_depth_mm": (0, 1000),
        "weather_code": (0, 101),
        "cloud_cover_%": (0, 100),
        "visibility_m": (0, 100000)
        # "evapotranspiration_mm": (0, 50),
        # "et0_fao_evapotranspiration_mm": (0, 50),
        # "vapour_pressure_deficit_kpa": (0, 10),
        # "mean_sea_level_pressure_hpa": (870, 1085),
        # "surface_pressure_hpa": (870, 1085)
    }
    df['is_outlier'] = False
    for col, (min_val, max_val) in thresholds.items():
        if col in df.columns:
            df['is_outlier'] |= (df[col] < min_val) | (df[col] > max_val)


    #delete if multiple data coming from same city.
    df.drop_duplicates(subset=['city'], inplace=True)


    #drop if imp cols have null values
    df.dropna(subset=["city","temperature_celcius","weather_code"],inplace=True)


    #audit trial cols
    df['processing_status'] = 'cleaned'

    print(df)
    # sucess message
    print('✅ Data cleaning completed successfully for weather alert system.')
    return df.to_dict(orient='records')
