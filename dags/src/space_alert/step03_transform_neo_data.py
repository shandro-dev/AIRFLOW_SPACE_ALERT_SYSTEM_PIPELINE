#DATA TRANSFORMATION
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
from datetime import datetime
import numpy as np


def transform_data(data):
    df=data

    #unique column as data_id combination of date time(including miliseconds)
    def generate_data_id(nasa_id):
        now = datetime.now()
            # Format date and time as required (2 digits each, millisecond 2 digits)
        dt_str = now.strftime('%d%m%y%H%M%S')  # day, month, year, hour, minute, second (all 2 digits)
        ms_str = str(int(now.microsecond / 10000)).zfill(2)  # convert microsecond to 2 digit millisecond (0-99)
        return f"{nasa_id}-{dt_str}{ms_str}"
    df['data_id'] = df['nasa_id'].apply(generate_data_id)



    #add data_load_datetime for data load date and time 
    df['created_at'] = datetime.now()



    #verify url and correction
    df['nasa_site_url'] = df['nasa_site_url'].apply(
        lambda url: url if isinstance(url, str) and url.startswith('https://') and 'nasa.gov' in url
        else 'url not found'
    )



    #handle is_potentially_hazardous boolean value and fill if needed
    def determine_hazard(row):
        val = str(row['is_potentially_hazardous']).strip().lower()
        if val in ['true', 'yes', '1']:
            return True
        elif val in ['false', 'no', '0']:
            return False
        else:
            # Check other conditions
            if (row['diameter_max_m'] > 150 and
                row['closest_approach_distance_km'] < 1000000 and
                row['velocity_kmph'] > 8000):
                return True
            else:
                return False
    df['is_potentially_hazardous'] = df.apply(determine_hazard, axis=1)



    #recheck duplicate based on data_id and drop if duplicate found.
    if not df['data_id'].is_unique:
        print("Duplicates found in 'data_id'. Removing duplicate rows, keeping the first occurrence.")
        df = df.drop_duplicates(subset=['data_id'], keep='first')
    else:
        pass



    columns_imp = [
        'asteroid_name', 'nasa_id', 'nasa_site_url', 'closest_approach_time_to_earth_IST',
        'closest_approach_distance_km', 'velocity_kmph', 'diameter_min_m', 'diameter_max_m',
        'is_potentially_hazardous','data_id','created_at'
    ]
    df['is_deleted'] = df[columns_imp].isnull().any(axis=1)



    # Add audit trail columns
    df['processing_status'] = 'transformed'
    df['batch_id'] = datetime.now().strftime('%Y%m%d%H%M%S') + f"{datetime.now().microsecond // 1000:03d}"



    # Add velocity catagory column
    def categorize_speed(speed):
        if speed < 25000:
            return "slow"
        elif speed < 65000:
            return "moderate"
        else:
            return "fast"
    df['velocity_category'] = df['velocity_kmph'].apply(categorize_speed)



    # Add hazard score based on velocity and size
    scaler = MinMaxScaler()
    df['hazard_score'] = scaler.fit_transform(
        (df['velocity_kmph'] * df['diameter_max_m']).values.reshape(-1, 1)
    ).round(3)



    # Add customized risk level
    bins = [0, 0.2, 0.5, 0.8, 1.0]
    labels = ['Low', 'Medium', 'High', 'Critical']
    df['risk_level'] = pd.cut(df['hazard_score'], bins=bins, labels=labels, include_lowest=True)



    # Add custom asteriod size category
    df['size_category'] = pd.cut(df['diameter_max_m'], bins=[0, 70, 180, 350, np.inf], labels=['small', 'medium', 'large', 'very_large'])



    # Add close_approch to check if metior is nearby earth
    df['is_close'] = (df['closest_approach_distance_km'] < 750000)



    # columns re-ordering
    sorted_columns=[
        'nasa_id',
        'asteroid_name',
        'closest_approach_time_to_earth_IST',
        'closest_approach_distance_km',
        'velocity_kmph',
        'diameter_min_m',
        'diameter_max_m',
        'nasa_site_url',
        'is_potentially_hazardous',
        'velocity_category',
        'hazard_score',
        'risk_level',
        'size_category',
        'is_close',
        'is_missing_data',
        'is_outlier',
        'is_deleted',
        'processing_status',
        'created_at',
        'data_id',
        'batch_id'
    ]
    df = df[sorted_columns]



    print('✅ Data transformation completed successfully for space alert system.')
    return df