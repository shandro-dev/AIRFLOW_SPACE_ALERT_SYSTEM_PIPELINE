#CLEAN DATA

import pandas as pd
from datetime import datetime



def clean_data(data):
    df=pd.DataFrame(data)

    # Data Type Optimization
    df['asteroid_name'] = df['asteroid_name'].astype(str)
    df['nasa_id'] = df['nasa_id'].astype(str)
    df['nasa_site_url'] = df['nasa_site_url'].astype(str)
    df['is_potentially_hazardous'] = df['is_potentially_hazardous'].astype(bool)



    #round off int/float columns
    df['closest_approach_distance_km'] = df['closest_approach_distance_km'].round(2)
    df['velocity_kmph'] = df['velocity_kmph'].round(2)
    df['diameter_min_m'] = df['diameter_min_m'].round(2)
    df['diameter_max_m'] = df['diameter_max_m'].round(2)



    #time ordering -> YYYY-MM-DD HH:MM:SS
    df['closest_approach_time_to_earth_IST'] = pd.to_datetime(df['closest_approach_time_to_earth_IST'], format='%Y-%b-%d %H:%M')



    #chnage UTC to IST
    df['closest_approach_time_to_earth_IST'] = df['closest_approach_time_to_earth_IST'] + pd.Timedelta(hours=5, minutes=30)



    #cleaning asteroid_name column and removing brackets
    df['asteroid_name'] = df['asteroid_name'].astype(str).str.replace(r'[()]', '', regex=True)



    #uppercasing asteroid_name
    df['asteroid_name'] = df['asteroid_name'].str.upper()  



    #drop duplicates
    df.drop_duplicates(subset=['nasa_id'], inplace=True)



    #missing values check
    critical_columns = ["asteroid_name","nasa_id","closest_approach_time_to_earth_IST","closest_approach_distance_km","velocity_kmph","diameter_max_m","is_potentially_hazardous"]
    df['is_missing_data'] = df[critical_columns].isnull().any(axis=1)



    #outliers check
    thresholds = {
        "closest_approach_distance_km": (100000, 110000000),
        "velocity_kmph":(7000,99000)
    }
    df['is_outlier'] = False
    for col, (min_val, max_val) in thresholds.items():
        if col in df.columns:
            df['is_outlier'] |= (df[col] < min_val) | (df[col] > max_val)



    #drop if imp cols have null values
    df.dropna(subset=["nasa_id","closest_approach_distance_km"],inplace=True)



    #audit trail column
    df['processing_status'] = 'cleaned'



    print('✅ Data cleaning completed successfully for space alert system.')
    return df