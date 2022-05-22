import pandas as pd
import numpy as np
import requests

def check_if_valid_data(df: pd.DataFrame, API_CITIES: list) -> bool:
    # Check if dataframe is empty
    if df.empty:
        raise Exception("API returned empty dataframe")

    # Primary Key Check
    if df.shape[0] != len(API_CITIES):
        raise Exception("API returned more rowws than the number of cities")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    if not np.array_equal(df['city'].values, API_CITIES):
        raise Exception("API returned wrong cities")

    return True


def run_weather_etl():
  DATABASE_LOCATION = "sqlite:///weather_data.sqlite"
  API_KEY           = "d79170c096d0407997175700222205"
  API_CITIES        = ["Saint-Malo", "Paris", "Port-Navalo"]

    # Create dict with final fields to be inserted
    data_dict = {
        "city": [],
        "region": [],
        "country": [],
        "latitude": [],
        "longitude": [],
        "timezone": [],
        "localtime": [],

        "last_updated": [],
        "temp_c": [],
        "condition_text": [],
        "wind_mph": [],
        "wind_degree": [],
        "wind_dir": [],
        "pressure_mb": [],
        "precip_mm": [],
        "humidity": [],
        "cloud": [],
        "feelslike_c": [],
        "vis_km": [],
        "uv": [],
        "gust_mph": [],
        "aqi_co": [],
        "aqi_no2": [],
        "aqi_o3": [],
        "aqi_so2": [],
        "aqi_pm2_5": [],
        "aqi_pm10": []
        }
    
    # API call foreach cities
    for api_city in API_CITIES:
        r = requests.get("https://api.weatherapi.com/v1/current.json?key={}&q={}&aqi=yes".format(API_KEY, api_city))

        data = r.json()

        data_dict["city"].append(data["location"]["name"])
        data_dict["region"].append(data["location"]["region"])
        data_dict["country"].append(data["location"]["country"])
        data_dict["latitude"].append(data["location"]["lat"])
        data_dict["longitude"].append(data["location"]["lon"])
        data_dict["timezone"].append(data["location"]["tz_id"])
        data_dict["localtime"].append(data["location"]["localtime"])

        data_dict["last_updated"].append(data["current"]["last_updated"])
        data_dict["temp_c"].append(data["current"]["temp_c"])
        data_dict["condition_text"].append(data["current"]["condition"]["text"])
        data_dict["wind_mph"].append(data["current"]["wind_mph"])
        data_dict["wind_degree"].append(data["current"]["wind_degree"])
        data_dict["wind_dir"].append(data["current"]["wind_dir"])
        data_dict["pressure_mb"].append(data["current"]["pressure_mb"])
        data_dict["precip_mm"].append(data["current"]["precip_mm"])
        data_dict["humidity"].append(data["current"]["humidity"])
        data_dict["cloud"].append(data["current"]["cloud"])
        data_dict["feelslike_c"].append(data["current"]["feelslike_c"])
        data_dict["vis_km"].append(data["current"]["vis_km"])
        data_dict["uv"].append(data["current"]["uv"])
        data_dict["gust_mph"].append(data["current"]["gust_mph"])
        data_dict["aqi_co"].append(data["current"]["air_quality"]["co"])
        data_dict["aqi_no2"].append(data["current"]["air_quality"]["no2"])
        data_dict["aqi_o3"].append(data["current"]["air_quality"]["o3"])
        data_dict["aqi_so2"].append(data["current"]["air_quality"]["so2"])
        data_dict["aqi_pm2_5"].append(data["current"]["air_quality"]["pm2_5"])
        data_dict["aqi_pm10"].append(data["current"]["air_quality"]["pm10"])

    df_weather = pd.DataFrame(data_dict)

    # Validate
    if check_if_valid_data(df_weather, API_CITIES):
       print("Data valid, proceed to Load stage")

    # Load
    df_weather.to_gbq("weather_data.history_sync", project_id="sandbox-hrialan", if_exists='append')
    
    print("Data loaded successfully")
