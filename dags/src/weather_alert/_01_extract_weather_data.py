# Fetch hourly weather info through open-meteo API

import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import json
from datetime import datetime, timezone

def weatherapi(latitude, longitude,location_name):
    # Setup session with caching and retry logic
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    client = openmeteo_requests.Client(session=retry_session)

    # API endpoint and parameters
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": [
            "temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature","wind_speed_10m",
            "precipitation_probability", "precipitation", "rain", "showers", "snowfall",
            "snow_depth", "weather_code", "pressure_msl", "surface_pressure", "cloud_cover",
            "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "visibility",
            "evapotranspiration", "et0_fao_evapotranspiration", "vapour_pressure_deficit"
        ]
    }

    # Fetch weather data
    responses = client.weather_api(url, params=params)
    response = responses[0]
    hourly = response.Hourly()

    # Generate datetime range for all hourly data points
    time_range = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )

    # Find current UTC hour rounded down
    now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    # Find the index of the current hour in the time_range
    try:
        current_index = time_range.get_loc(now_utc)
    except KeyError:
        # If current time not in forecast range, fallback to nearest time (first available)
        current_index = 0

    # Prepare data for the current hour only
    current_hour_data = {"city": location_name}

    variables = [
        "temperature_celcius", "humidity_%", "dew_temperature_celcius", "feels_like_temperature_celcius","wind_speed_kmph",
        "precipitation_%", "precipitation_occured_mm", "rain_mm", "showers_mm", "snowfall_mm",
        "snow_depth_mm", "weather_code", "mean_sea_level_pressure_hpa", "surface_pressure_hpa", "cloud_cover_%",
        "visibility_m",
        "evapotranspiration_mm", "et0_fao_evapotranspiration_mm", "vapour_pressure_deficit_kpa"
    ]

    # Extract the value for the current hour from each variable
    for i, var in enumerate(variables):
        values = hourly.Variables(i).ValuesAsNumpy()
        current_hour_data[var] = float(values[current_index]) if values.size > current_index else None

    return current_hour_data

def fetch_weather_batch():
    delhi=weatherapi(28.7041,77.1025,'Delhi')

    mumbai=weatherapi(18.9582,72.8321,'Mumbai')

    bengaluru=weatherapi(12.9629,77.5775,'Bengaluru')

    hyderabad=weatherapi(17.4065, 78.4772,'Hyderabad')

    chennai=weatherapi(13.0843,80.2705,'Chennai')

    kolkata=weatherapi(22.5744,88.3629,'Kolkata')

    ahmedabad=weatherapi(23.0225,72.5714,'Ahmedabad')

    pune=weatherapi(18.5246,73.8786,'Pune')

    jaipur=weatherapi(26.9124,75.7873,'Jaipur')

    lucknow=weatherapi(26.8467,80.9462,'Lucknow')
    
    all_cities=[delhi,mumbai,bengaluru,hyderabad,chennai,kolkata,ahmedabad,pune,jaipur,lucknow]

    # debugging
    print(f"✅ Data fetched successfully from API for weather alert system")


    return all_cities

print(fetch_weather_batch())





"""
    Fetches current hourly weather data for a specified location using the Open-Meteo API.
    
    Args:
        latitude (float): Latitude of the location.
        longitude (float): Longitude of the location.
        location_name (str): Human-readable name of the location (for reference).
        
    Returns:
        dict: Weather data for the current hour, including temperature, humidity, precipitation,
              cloud cover, and other atmospheric variables.
              
    Implementation details:
        - Uses a cached session with automatic retries to improve API request reliability.
        - Fetches hourly forecast data from Open-Meteo API for a wide range of weather variables.
        - Converts API response timestamps to pandas datetime for easier time handling.
        - Extracts the data corresponding to the current UTC hour.
        - Returns a dictionary with all requested weather parameters for the current hour.

        
Data Dictionary Explanation:

- location: Name of the city (string).
- date: Date and time of the weather reading in 'YYYY-MM-DD HH:MM:SS' format (string).
- temperature_2m: Air temperature measured 2 meters above the ground, in degrees Celsius (float).
- relative_humidity_2m: Relative humidity at 2 meters height, in percentage (%) (float).
- dew_point_2m: Dew point temperature at 2 meters height, in degrees Celsius (float).
- apparent_temperature: Feels-like temperature considering humidity and wind, in degrees Celsius (float).
- wind_speed_10m: wind speed in kmph above 10 meter of ground level(float).
- precipitation_probability: Probability of precipitation occurring, in percentage (%) (float).
- precipitation: Amount of precipitation expected or recorded, in millimeters (mm) (float).
- rain: Amount of rain specifically, in millimeters (mm) (float).
- showers: Amount of showers, in millimeters (mm) (float).
- snowfall: Amount of snowfall, in millimeters (mm) (float).
- snow_depth: Depth of snow on the ground, in millimeters (mm) (float).
- weather_code: Numerical code representing weather conditions (e.g., clear, rain, snow) (float).
- pressure_msl: Atmospheric pressure at mean sea level, in hectopascals (hPa) (float).
- surface_pressure: Atmospheric pressure at the surface, in hectopascals (hPa) (float).
- cloud_cover: Total cloud cover percentage (%) (float).
- cloud_cover_low: Low-level cloud cover percentage (%) (float).
- cloud_cover_mid: Mid-level cloud cover percentage (%) (float).
- cloud_cover_high: High-level cloud cover percentage (%) (float).
- visibility: Visibility distance in meters (float).
- evapotranspiration: Amount of evapotranspiration, representing water transfer from land to atmosphere, in millimeters (mm) (float).
- et0_fao_evapotranspiration: Reference evapotranspiration (FAO standard), in millimeters (mm) (float).
- vapour_pressure_deficit: Difference between saturation and actual vapor pressure, indicating dryness of air, in kilopascals (kPa) (float).

"""
