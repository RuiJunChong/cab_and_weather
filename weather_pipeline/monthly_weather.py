import pandas as pd

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, year, month

import requests
from datetime import datetime
import schedule
import time


spark = (
    SparkSession.builder
    .appName("monthly_weather")
    .getOrCreate()
)

postgres_url = "jdbc:postgresql://localhost:5432/cab_db"
properties = {"user": "postgres",
              "password": "Postgres12_",
              "driver": "org.postgresql.Driver"}


def get_data():
    now = datetime.now()
    
    if now.day == 1:
        # get tripdata
        res = requests.get(f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{now.strftime("%Y-%m")}.parquet")
        with open(f"yellow_tripdata_{now.strftime("%Y%m")}.parquet", "wb") as f:
            f.write(res.content)
            
        tripdata_df = spark.read.parquet(f"yellow_tripdata_{now.strftime("%Y%m")}.parquet")
        
        tripdata_df = tripdata_df.filter(year(col("tpep_pickup_datetime")) == now.year).filter(month(col("tpep_pickup_datetime")) == now.month)
        tripdata_df = tripdata_df.withColumn("pickup_date", date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd"))
        tripcount_df = tripdata_df.groupBy("pickup_date").count() 
        
        # get weather data
        weather_df = spark.read.jdbc(url=postgres_url,
                                    table="DAILY_WEATHER",
                                    properties=properties)
        
        weather_df = weather_df.filter(year(col("date")) == now.year).filter(month(col("date")) == now.month)
        
        # join tripcount and weather data
        joined_df = weather_df.join(tripcount_df, tripcount_df.pickup_date == weather_df.date, how="left")
        
        joined_df = joined_df.orderBy("date")
        joined_df = joined_df.select('date',
                                    'count',
                                    'temperature_2m_max',
                                    'temperature_2m_min',
                                    'temperature_2m_mean',
                                    'daylight_duration',
                                    'sunshine_duration',
                                    'precipitation_sum',
                                    'precipitation_hours',
                                    'wind_speed_10m_max',
                                    'wind_gusts_10m_max',
                                    )
        joined_df = joined_df.withColumnRenamed("count", "trip_count")
        
        joined_df.write.jdbc(url=postgres_url,
                            table="MONTHLY_TRIPCOUNT_WEATHER",
                            mode="append",
                            properties=properties)
    
    
schedule.every().day.at("03:00").do(get_data)

while True:
    schedule.run_pending()
    time.sleep(1)