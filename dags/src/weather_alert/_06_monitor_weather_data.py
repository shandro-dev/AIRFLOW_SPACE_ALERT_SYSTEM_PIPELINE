# MONITOR DATA

def monitor_weather_events(data):
    df=data
    extreme_events = []
    for idx, row in df.iterrows():
        events = []

        if (
            row['wind_speed_kmph'] > 50 and
            row['precipitation_%'] > 80 and
            row['precipitation_occured_mm'] > 10 and
            row['cloud_cover_%'] > 90 and
            row['visibility_m'] < 1000
        ):
            events.append("Heavy Thunderstorm / Storm")

        if (
            row['wind_speed_kmph'] > 90 and
            row['mean_sea_level_pressure_hpa'] < 990 and
            row['visibility_m'] < 800
        ):
            events.append("Cyclone / Severe Windstorm")

        if row['temperature_celcius'] > 45 and row['humidity_%'] < 30:
            events.append("Heatwave")

        if row['temperature_celcius'] < 5 and row['humidity_%'] > 70:
            events.append("Cold Wave")

        if (row['rain_mm'] + row['showers_mm']) > 50:
            events.append("Heavy Rainfall")

        if (
            (row['snowfall_mm'] > 20 or row['snow_depth_mm'] > 50) and
            row['wind_speed_kmph'] > 30
        ):
            events.append("Snowstorm / Blizzard")

        if row['visibility_m'] < 40:
            events.append("Dense Fog")

        if events:
            extreme_events.append({
                "city": row['city'],
                "events": events
            })
        if_events = 'YES' if not events else 'NO'

    return extreme_events

# Run only if load is successful
# if not detect_extreme_weather_conditions(df):
#     chk = "NO"
# else:
#     chk = "YES"

# if load_dataframe_to_postgres(df):
#     alerts = detect_extreme_weather_conditions(df)
#     if not alerts:
#         chk = "NO"
#     else:
#         chk = "YES"
#     print(f"✅ Weather alerts detected: {chk} for weather alert system")
#     # print(alerts)
# else:
#     print("❌ Execution failed with exit code 1.")# 
