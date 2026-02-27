import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
load_dotenv()

def neosapi():
    nasa_neo_api_key = os.getenv("NASA_NEO_API_KEY") # <-- your API key here

    now = datetime.utcnow()
    start_date = (now - timedelta(days=0.5)).strftime("%Y-%m-%d")
    end_date = (now + timedelta(days=0.5)).strftime("%Y-%m-%d")

    url = "https://api.nasa.gov/neo/rest/v1/feed"
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "api_key": nasa_neo_api_key
    }

    response = requests.get(url, params=params)
    data = response.json()

    neos_list = []

    for date in data.get("near_earth_objects", {}):
        for neo in data["near_earth_objects"][date]:
            approach_data = neo.get("close_approach_data", [])
            if approach_data:
                approach_time = approach_data[0].get("close_approach_date_full")
                neos_list.append({
                    "asteroid_name": neo["name"],
                    "nasa_id": neo["id"],
                    "nasa_site_url": neo["nasa_jpl_url"],
                    "closest_approach_time_to_earth_IST": approach_time,
                    "closest_approach_distance_km": float(approach_data[0]["miss_distance"]["kilometers"]),
                    "velocity_kmph": float(approach_data[0]["relative_velocity"]["kilometers_per_hour"]),
                    "diameter_min_m": float(neo["estimated_diameter"]["meters"]["estimated_diameter_min"]),
                    "diameter_max_m": float(neo["estimated_diameter"]["meters"]["estimated_diameter_max"]),
                    "is_potentially_hazardous": neo["is_potentially_hazardous_asteroid"]
                })
    print('✅ Data fetched successfully for space alert system.')
    return neos_list

# print(neosapi())




"""
    Fetches Near-Earth Object (NEO) data from NASA's public API within a
    half-day window around the current UTC time (12 hours before and after now).
    
    Returns:
        A list of dictionaries, each containing detailed information about
        a Near-Earth Object expected to approach Earth in the specified time frame.
    
    Each dictionary contains:
        - name: The official name of the NEO.
        - id: Unique identifier of the NEO.
        - nasa_jpl_url: URL linking to NASA's Jet Propulsion Laboratory page for more info.
        - close_approach_time: Exact date and time when the NEO will closely approach Earth.
        - miss_distance_km: Closest distance the NEO will pass from Earth, in kilometers.
        - velocity_kph: Speed of the NEO relative to Earth, in kilometers per hour.
        - diameter_min_m: Estimated minimum diameter of the NEO, in meters.
        - diameter_max_m: Estimated maximum diameter of the NEO, in meters.
        - is_potentially_hazardous: Boolean indicating if the NEO is classified as potentially hazardous.
    
    Note:
        - Requires a valid NASA API key.
        - Uses NASA's NEO Feed API endpoint.
"""