#https://github.com/karolina-sowinska/free-data-engineering-course-for-beginners/blob/master/main.py

import pandas as pd
import requests
import json

API_KEY = "d79170c096d0407997175700222205"
API_CITY = "Saint-Malo"

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        raise Exception("API returned empty dataframe")

    # Primary Key Check
    if df.shape[0] != 1:
        raise Exception("API returned more than one row")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    if df['city'].values[0] != API_CITY:
        raise Exception("API returned wrong city")

    return True


if __name__ == "__main__":

    r = requests.get("https://api.weatherapi.com/v1/current.json?key={}&q={}&aqi=yes".format(API_KEY, API_CITY))

    data = r.json()

    data_dict = {
        "city": data['location']['name'],
        "region": data['location']['region'],
        "country": data['location']['country'],
        "latitude": data['location']['lat'],
        "longitude": data['location']['lon'],
        "timezone": data['location']['tz_id'],
        "localtime": data['location']['localtime'],

        "last_updated": data['current']['last_updated'],
        "temp_c": data['current']['temp_c'],
        "condition_text": data['current']['condition']['text'],
        "wind_mph": data['current']['wind_mph'],
        "wind_degree": data['current']['wind_degree'],
        "wind_dir": data['current']['wind_dir'],
        "pressure_mb": data['current']['pressure_mb'],
        "precip_mm": data['current']['precip_mm'],
        "humidity": data['current']['humidity'],
        "cloud": data['current']['cloud'],
        "feelslike_c": data['current']['feelslike_c'],
        "vis_km": data['current']['vis_km'],
        "uv": data['current']['uv'],
        "gust_mph": data['current']['gust_mph'],
        "aqi_co": data['current']['air_quality']['co'],
        "aqi_no2": data['current']['air_quality']['no2'],
        "aqi_o3": data['current']['air_quality']['o3'],
        "aqi_so2": data['current']['air_quality']['so2'],
        "aqi_pm2_5": data['current']['air_quality']['pm2_5'],
        "aqi_pm10": data['current']['air_quality']['pm10'],
    }

    df_weather = pd.DataFrame(data_dict, index=[0])

    # Validate
    if check_if_valid_data(df_weather):
        print("Data valid, proceed to Load stage")