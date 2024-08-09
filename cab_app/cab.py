from flask import Flask, render_template, request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, hour, format_string, avg, round, format_number
import psycopg2

import requests
import pandas as pd
from datetime import datetime

import os

app = Flask(__name__)

postgres_url = "jdbc:postgresql://localhost:5432/cab_db"
properties = {"user": "postgres",
              "password": "Postgres12_",
              "driver": "org.postgresql.Driver"}

spark = (
    SparkSession.builder
    .appName("yellow_tripdata")
    .getOrCreate()
)


def get_data(dt):
    # check if data is in the table
    conn = psycopg2.connect(dbname="cab_db",
                            user="postgres",
                            password="Postgres12_",
                            host="localhost",
                            port="5432")

    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute(f"""
        SELECT EXISTS (
            SELECT 1
            FROM (SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public')
            WHERE table_name = 'yellow_tripdata_{dt.strftime("%Y%m")}'
        ) AS exists_value;
    """)

    results = cursor.fetchall()

    print(f"Date Table Exists: {results[0][0]}")    # first row, first column
    
    if results[0][0]:
        # get table data if table exists               
        df = spark.read.jdbc(
            url=postgres_url,
            table=f'yellow_tripdata_{dt.strftime("%Y%m")}',
            properties=properties
        )
        
    else:
        # Download the data
        download_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{dt.strftime("%Y-%m")}.parquet"
        print(f"Downloading data for {dt.strftime("%Y-%m")} from {download_url}...")
        
        res = requests.get(download_url)
        
        with open(f"cab_and_weather/yellow_tripdata_{dt.strftime("%Y%m")}.parquet", "wb") as f:
            f.write(res.content)
            
        print("Data downloaded successfully!")
            
        df = spark.read.parquet(f"cab_and_weather/yellow_tripdata_{dt.strftime("%Y%m")}.parquet")
        
        # get the number of tables in the database
        cursor.execute("""
            SELECT COUNT(*)
            FROM (SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                        AND table_name LIKE 'yellow_tripdata_%')
        """)
        
        table_count = int(cursor.fetchall()[0][0])
        print(f"Number of tables in the database: {table_count}")
        
        if table_count == 3:
            print("Dropping the oldest table...")
            
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                    AND table_name LIKE 'yellow_tripdata_%'
                ORDER BY table_name
                LIMIT 1
            """)
            
            oldest_table = cursor.fetchall()[0][0]
            
            cursor.execute(f"DROP TABLE {oldest_table}")
            
            file_path = f"cab_and_weather/{oldest_table}.parquet"
            if os.path.exists(file_path):
                os.remove(file_path)
                
            
            print(f"{oldest_table} dropped successfully!")
        
        df.write.jdbc(url=postgres_url, 
                        table=f"YELLOW_TRIPDATA_{dt.strftime("%Y%m")}", 
                        mode="overwrite", 
                        properties=properties)
        
        print(f"Data for {dt.strftime('%Y-%m')} saved to database!")
            
    cursor.close()
    conn.close()
    
    return df.filter(date_format(col("TPEP_PICKUP_DATETIME"), "yyyyMMdd") == dt.strftime("%Y%m%d"))


def get_html_output(df, pickup_id, dropoff_id, max_distance):
    # if pickup and dropoff location is specified
    if pickup_id and dropoff_id:
        df = df.filter(col("PULOCATIONID") == pickup_id)
        df = df.filter(col("DOLOCATIONID") == dropoff_id)
        if max_distance:
            df = df.filter(col("TRIP_DISTANCE") <= max_distance)
        
        if df.count() == 0:
            return "No data found for the specified pickup and dropoff location"
        
        hourly_df = df.withColumn("hour", hour(col("TPEP_PICKUP_DATETIME")))
        hourly_df = hourly_df.withColumn("hour", format_string("%02d:00", col("hour")))
        avg_fare = hourly_df.groupBy("hour").agg(avg("TOTAL_AMOUNT").alias("average_fare")).orderBy("average_fare")
        avg_fare = avg_fare.withColumn("average_fare", format_number(col("average_fare"), 2))
        cheapest = (avg_fare.first()["hour"], avg_fare.first()["average_fare"])
        avg_fare = avg_fare.orderBy("hour")
        
        data = {"title": "Average Fare For Each Hour"}
        if max_distance:
            data["title"] += f" (Distance <= {max_distance} miles)"
        data["cheapest"] = cheapest
        data["location"] = (pickup_id, dropoff_id)
        data["table_data"] = avg_fare.toPandas().to_dict(orient='records')
        
        return render_template('average_fare.html', data=data)
    
    # if only date is specified
    else:
        hourly_df = df.withColumn("hour", hour(col("TPEP_PICKUP_DATETIME")))
        hourly_df = hourly_df.withColumn("hour", format_string("%02d:00", col("hour")))
        hourly_trip_count = hourly_df.groupBy("hour").count().orderBy("hour")
        
        data = {"title": "Trip Count For Each Hour"}
        data["table_data"] = hourly_trip_count.toPandas().to_dict(orient='records')
        
        return render_template('hourly_count.html', data=data)


@app.route('/')
def index():
    date_input = request.args.get('date')
    pickup_id = request.args.get("puid")
    dropoff_id = request.args.get("doid")
    max_distance = request.args.get("max_distance")
    
    if date_input:
        try:
            dt = datetime.strptime(date_input, '%Y%m%d')
        except ValueError:
            return "Incorrect date format, should be YYYYMMDD"
        
        if dt.year < 2009 or dt > datetime(2024, 5, 31):
            return "Date out of range. Please enter a date between 2009-01-01 and 2024-05-31"
    
    else:
        return "Please enter a date parameter in the format YYYYMMDD"
    
    if pickup_id or dropoff_id:
        if not pickup_id or not dropoff_id:
            return "Please enter both pickup and dropoff ID"
        
        try:
            pickup_id = int(pickup_id)
            dropoff_id = int(dropoff_id)
        except ValueError:
            return "Invalid pickup or dropoff ID. Please enter a valid integer"
        
        if pickup_id < 1 or pickup_id > 265 or dropoff_id < 1 or dropoff_id > 265:
            return "Invalid pickup or dropoff ID. Please enter a valid integer between 1 and 265"
        
    df = get_data(dt)
    
    return get_html_output(df, pickup_id, dropoff_id, max_distance)
    
   

if __name__ == '__main__':
    app.run(debug=True)
