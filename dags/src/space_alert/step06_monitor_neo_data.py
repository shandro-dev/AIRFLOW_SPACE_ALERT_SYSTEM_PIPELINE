 # MONITOR DATA
# from .step04_validate_neo_data import df
# from .step05_load_neo_data import load_dataframe_to_postgres

def monitor_neo(data):
    df=data
    critical_events = []

    for idx, row in df.iterrows():
        events = []

        if row['closest_approach_distance_km'] < 1500000:
            events.append("🔴 Very Close Approach")
        if row['velocity_kmph'] > 100000:
            events.append("🔴 Extremely High Velocity")
        if row['diameter_max_m'] > 400:
            events.append("🔴 Large Asteroid")
        if row['hazard_score'] >= 0.8:
            events.append("🔴 High Hazard Score")
        if row['is_potentially_hazardous']:
            events.append("🔴 NASA-Flagged Hazard")

        if events:

            time_val = row['closest_approach_time_to_earth_IST']
            if hasattr(time_val, 'isoformat'):
                time_val = time_val.isoformat()
            else:
                time_val = str(time_val)

            critical_events.append({
                "nasa_id": row['nasa_id'],
                "asteroid_name": row['asteroid_name'],
                "closest_approach_time_to_earth_IST": time_val, # Now it's a string!
                "alerts": events,
                "url": row['nasa_site_url']
            })

    return critical_events


# Load and monitor only if load was successful
# load_success = load_dataframe_to_postgres(df)

# if load_success:
#     alerts = detect_near_earth_threats(df)
#     chk = "YES" if alerts else "NO"
#     print(f"✅ Asteroid alerts detected: {chk} for space alert system")
#     # print(alerts)  # Uncomment to log full alert details
# else:
#     print("❌ Execution failed with exit code 1.")
