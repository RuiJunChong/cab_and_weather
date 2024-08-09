import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, DateType

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)


# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
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

daily_dataframe = pd.DataFrame(data = daily_data)

spark = (
    SparkSession.builder
    .appName("weather_data")
    .getOrCreate()
)

# postgresql connection for pyspark
postgres_url = "jdbc:postgresql://localhost:5432/cab_db"
properties = {"user": "postgres",
              "password": "Postgres12_",
              "driver": "org.postgresql.Driver"}

daily_dataframe.to_csv('daily_weather.csv', index=False)
weather_df = spark.read.csv('daily_weather.csv', header=True)

weather_df = (weather_df
              .withColumn("date", weather_df["date"].cast(DateType())) 
              .withColumn("temperature_2m_max", weather_df["temperature_2m_max"].cast(FloatType())) 
              .withColumn("temperature_2m_min", weather_df["temperature_2m_min"].cast(FloatType())) 
              .withColumn("temperature_2m_mean", weather_df["temperature_2m_mean"].cast(FloatType())) 
              .withColumn("daylight_duration", weather_df["daylight_duration"].cast(FloatType()))
              .withColumn("sunshine_duration", weather_df["sunshine_duration"].cast(FloatType()))
              .withColumn("precipitation_sum", weather_df["precipitation_sum"].cast(FloatType()))
              .withColumn("precipitation_hours", weather_df["precipitation_hours"].cast(FloatType()))
              .withColumn("wind_speed_10m_max", weather_df["wind_speed_10m_max"].cast(FloatType()))
              .withColumn("wind_gusts_10m_max", weather_df["wind_gusts_10m_max"].cast(FloatType()))
              )


weather_df.write.jdbc(url=postgres_url, 
                        table="DAILY_WEATHER", 
                        mode="append", 
                        properties=properties)


df = spark.read.jdbc(url=postgres_url,
                    table="DAILY_WEATHER",
                    properties=properties)




