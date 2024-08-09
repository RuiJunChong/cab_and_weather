import openmeteo_requests

import requests_cache
import pandas as pd
import numpy as np
from retry_requests import retry

import psycopg2
from psycopg2.extensions import register_adapter, AsIs
from datetime import datetime
import schedule
import time


# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)


def get_weather_data():
    datetime_now = datetime.now()
    print(f"Running weather data insert for {datetime_now.strftime('%Y-%m-%d')}")
    
    # Make sure all required weather variables are listed here
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 40.7143,
        "longitude": -74.006,
        "start_date": "2024-01-01",
        "end_date": "2024-08-07",
        "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", "daylight_duration", 
            "sunshine_duration", "precipitation_sum", "precipitation_hours", "wind_speed_10m_max", "wind_gusts_10m_max"],
        "timezone": "America/New_York"
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_mean = daily.Variables(2).ValuesAsNumpy()
    daily_daylight_duration = daily.Variables(3).ValuesAsNumpy()
    daily_sunshine_duration = daily.Variables(4).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(5).ValuesAsNumpy()
    daily_precipitation_hours = daily.Variables(6).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(7).ValuesAsNumpy()
    daily_wind_gusts_10m_max = daily.Variables(8).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s") - pd.Timedelta(hours = 4),
        end = pd.to_datetime(daily.TimeEnd(), unit = "s") - pd.Timedelta(hours = 4),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left"
    )}
    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
    daily_data["daylight_duration"] = daily_daylight_duration
    daily_data["sunshine_duration"] = daily_sunshine_duration
    daily_data["precipitation_sum"] = daily_precipitation_sum
    daily_data["precipitation_hours"] = daily_precipitation_hours
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
    daily_data["wind_gusts_10m_max"] = daily_wind_gusts_10m_max

    daily_df = pd.DataFrame(data = daily_data)
    daily_df.fillna(-1, inplace=True)
    
    # check all tables in the database
    register_adapter(np.float32, AsIs)
    
    conn = psycopg2.connect(dbname="cab_db",
                            user="postgres",
                            password="Postgres12_",
                            host="localhost",
                            port="5432")

    conn.autocommit = True
    cursor = conn.cursor()
    
    columns = ", ".join(daily_df.columns.to_list())
    values = ", ".join(["%s"] * len(daily_df.columns))
    insert_query = f"""
        INSERT INTO daily_weather ({columns})
        VALUES ({values})
    """
    cursor.execute(insert_query, daily_df.iloc[0].values.tolist())
    
    print(f"Inserted daily weather data for {datetime_now.strftime('%Y-%m-%d')}")
    
    cursor.close()
    conn.close()


schedule.every().day.at("02:00").do(get_weather_data)

while True:
    schedule.run_pending()
    time.sleep(1)

