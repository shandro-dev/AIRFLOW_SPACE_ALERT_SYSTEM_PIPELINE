# VALIDATE DATA
import pandas as pd
from datetime import datetime

def validate_data(data):
    df=data
    errors = []

    # 1. Schema Validation
    required_columns = [
        'nasa_id', 'asteroid_name', 'closest_approach_time_to_earth_IST', 
        'closest_approach_distance_km', 'velocity_kmph', 
        'diameter_min_m', 'diameter_max_m', 'nasa_site_url', 
        'is_potentially_hazardous', 'velocity_category', 'hazard_score', 
        'risk_level', 'size_category', 'is_close', 'is_missing_data', 
        'is_outlier', 'is_deleted', 'processing_status', 'created_at', 
        'data_id', 'batch_id'
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        errors.append(f"Missing columns: {missing_columns}")

    # 2. Uniqueness Constraint
    if not df['nasa_id'].is_unique:
        errors.append("Duplicate nasa_id found")

    if not df['data_id'].is_unique:
        errors.append("Duplicate data_id found")

    # 3. Value Range Checks
    if (df['closest_approach_distance_km'] <= 0).any():
        errors.append("Invalid values in closest_approach_distance_km (should be > 0)")

    if (df['velocity_kmph'] <= 0).any():
        errors.append("Invalid values in velocity_kmph (should be > 0)")

    if (df['diameter_min_m'] < 0).any() or (df['diameter_max_m'] < 0).any():
        errors.append("Negative diameters detected")

    if (df['diameter_min_m'] > df['diameter_max_m']).any():
        errors.append("diameter_min_m greater than diameter_max_m")

    if (df['hazard_score'] < 0).any() or (df['hazard_score'] > 1).any():
        errors.append("hazard_score out of [0, 1] range")

    # 4. Categorical Consistency
    expected_velocity_cats = {'slow', 'moderate', 'fast'}
    if not df['velocity_category'].isin(expected_velocity_cats).all():
        errors.append("Unexpected values in velocity_category")

    expected_risk_levels = {'Low', 'Medium', 'High', 'Critical'}
    if not df['risk_level'].isin(expected_risk_levels).all():
        errors.append("Unexpected values in risk_level")

    expected_size_cats = {'small', 'medium', 'large', 'very_large'}
    if not df['size_category'].isin(expected_size_cats).all():
        errors.append("Unexpected values in size_category")

    # 5. created_at must not be in future
    if (df['created_at'] > datetime.now()).any():
        errors.append("created_at has future dates")

    # Final output
    if errors:
        print("❌ Validation FAILED with the following issues:")
        for err in errors:
            print("-", err)
        return False
    else:
        print("✅ Validation PASSED. Data is ready for downstream for space alert system.")
        return True


