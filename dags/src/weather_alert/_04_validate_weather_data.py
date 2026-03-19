#DATA VALIDATION
import pandas as pd
from datetime import datetime

def validate(data):

    df=pd.DataFrame(data)
    errors = []

    # 1. Schema / Column Presence
    required_columns = [
        'city', 'temperature_celcius', 'humidity_%', 'weather_code',
        'weather_id', 'created_at', 'is_deleted', 'processing_status', 'batch_id'
    ]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing columns: {missing_cols}")

    # 2. Data Type Checks (spot check)
    expected_types = {
        'city': str,
        'temperature_celcius': float,
        'humidity_%': float,
        'weather_code': (int, float),
        'weather_id': str,
        'created_at': pd.Timestamp,
    }
    for col, dtype in expected_types.items():
        if col in df.columns:
            if not df[col].map(lambda x: isinstance(x, dtype)).all():
                errors.append(f"Column {col} has wrong type(s)")

    # 3. Uniqueness Check
    if not df['weather_id'].is_unique:
        errors.append("Duplicate weather_id found")


    # 5. Range/Logic Check: temperature and humidity
    if df['temperature_celcius'].between(-90, 60).all() is False:
        errors.append("Temperature out of expected range")

    if df['humidity_%'].between(0, 100).all() is False:
        errors.append("Humidity out of expected range")

    # 6. Flag Consistency Check
    if not df[df['snowfall_mm'] > 0]['is_snowfall'].all():
        errors.append("is_snowfall mismatch")

    if not df[(df['rain_mm'] > 0) | (df['showers_mm'] > 0)]['is_rainfall'].all():
        errors.append("is_rainfall mismatch")

    if not df[df['visibility_m'] < 1000]['is_foggy'].all():
        errors.append("is_foggy mismatch")

    # 7. Date Check: created_at not in future
    if (df['created_at'] > datetime.now()).any():
        errors.append("created_at has future dates")


    # Final result
    if errors:
        print("❌ Validation FAILED with the following issues for weather alert system:")
        for err in errors:
            print("-", err)
        return False
    else:
        print("✅ Validation PASSED. Data is ready for downstream for weather alert system.")
        return True
    
