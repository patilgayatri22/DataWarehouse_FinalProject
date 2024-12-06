#Step 1 Import all required modules
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests
import pandas as pd
import numpy as np



#Connect to snowflake account

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

#Step to get data from Source

@task
def return_weatherdata():
  """
   - return the last few days data
  """
  vantage_api_key = Variable.get('weather_api_key')
  start_date = "2024-11-01"
  end_date = datetime.now().strftime("%Y-%m-%d")
  url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/california/{start_date}/{end_date}?unitGroup=metric&key={vantage_api_key}&contentType=json"
  r = requests.get(url)
  data = r.json()

  if "days" in data:
        return data["days"]
  else:
        raise ValueError("API response does not contain 'days'. Check the API response format.")




@task
def create_load_incremental(records):
    staging_table = "weather.source.daily_weather_stage"
    target_table = "weather.source.daily_weather"
    conn = return_snowflake_conn()
    try:
       conn.execute(f"""
               CREATE OR REPLACE TABLE {target_table} (
                datetime DATE,
                tempmax FLOAT,
                tempmin FLOAT,
                temp FLOAT,
                feelslike FLOAT,
                humidity FLOAT,
                precip FLOAT,
                windgust FLOAT,
                windspeed FLOAT,
                pressure FLOAT,
                cloudcover FLOAT,
                solarradiation FLOAT,
                severerisk INT,
                sunrise TIME,
                sunset TIME,
                conditions VARCHAR,
                PRIMARY KEY (datetime)
                );
            """)
          ## Create or replace the staging table
       conn.execute(f"""
             CREATE OR REPLACE TABLE {staging_table} (
                datetime DATE,
                tempmax FLOAT,
                tempmin FLOAT,
                temp FLOAT,
                feelslike FLOAT,
                humidity FLOAT,
                precip FLOAT,
                windgust FLOAT,
                windspeed FLOAT,
                pressure FLOAT,
                cloudcover FLOAT,
                solarradiation FLOAT,
                severerisk INT,
                sunrise TIME,
                sunset TIME,
                conditions VARCHAR,
                PRIMARY KEY (datetime)
                );
            """)

          # Insert records into the staging table
       for r in records:
                datetime = r['datetime']
                tempmax = r['tempmax']
                tempmin = r['tempmin']
                temp = r['temp']
                feelslike = r['feelslike']
                humidity = r['humidity']
                precip = r['precip']
                windgust = r['windgust']
                windspeed = r['windspeed']
                pressure = r['pressure']
                cloudcover = r['cloudcover']
                solarradiation = r['solarradiation']
                severerisk = r['severerisk']
                sunrise = r['sunrise']
                sunset = r['sunset']
                conditions = r['conditions']
                #insert_sql = f"INSERT INTO {staging_table} (datetime, tempmax, tempmin, temp, feelslike, humidity, precip, windspeed, cloudcover, conditions) VALUES ('{datetime}',{tempmax}, {tempmin}, {temp}, {feelslike}, {humidity}, {precip},{windspeed}, {cloudcover},{conditions})"
                #conn.execute(insert_sql) # Execute within the with block

                insert_sql = f"""INSERT INTO {staging_table} (datetime, tempmax, tempmin, temp, feelslike, humidity, precip, windgust, windspeed, pressure, cloudcover, solarradiation, 
                                severerisk, sunrise, sunset, conditions
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
                conn.execute(insert_sql, ( datetime, tempmax, tempmin, temp, feelslike, humidity, precip, windgust, windspeed,pressure, cloudcover, solarradiation, severerisk, sunrise, sunset, conditions))

       conn.execute("COMMIT;")  

        # perform UPSERT
       upsert_sql = f"""
            MERGE INTO {target_table} AS target
            USING {staging_table} AS stage
            ON target.datetime = stage.datetime
            WHEN MATCHED THEN
                UPDATE SET
                    target.datetime = stage.datetime,
                    target.tempmax = stage.tempmax,
                    target.tempmin = stage.tempmin,
                    target.temp = stage.temp,
                    target.feelslike = stage.feelslike,              
                    target.humidity = stage.humidity,
                    target.precip = stage.precip,
                    target.windgust = stage.windgust,
                    target.windspeed = stage.windspeed,
                    target.pressure = stage.pressure,
                    target.cloudcover = stage.cloudcover,
                    target.solarradiation = stage.solarradiation,
                    target.severerisk = stage.severerisk,
                    target.sunrise = stage.sunrise,
                    target.sunset = stage.sunset,
                    target.conditions = stage.conditions
            WHEN NOT MATCHED THEN
                INSERT ( datetime, tempmax, tempmin, temp, feelslike, humidity, precip,  windgust, windspeed, pressure, cloudcover, solarradiation,  severerisk, sunrise, sunset,  conditions)
                VALUES (stage.datetime, stage.tempmax, stage.tempmin, stage.temp, stage.feelslike, stage.humidity, stage.precip, stage.windgust, stage.windspeed, stage.pressure, stage.cloudcover, stage.solarradiation, stage.severerisk,stage.sunrise, stage.sunset,  stage.conditions)
                 """

       conn.execute(upsert_sql)
       #Commit the change
       conn.execute("COMMIT;")  
       print(f"Stage Table {staging_table}, Target table create '{target_table}', Data loaded successfully in both the tables using Incremental Load ")
    except Exception as e:
        conn.execute("ROLLBACK;")
        print(e)
        raise e



with DAG(
    dag_id = 'Pipeline_WEATHER_DATA_ETL',
    start_date = datetime(2024,11,28),
    catchup=False,
    tags=['ETL'],
    schedule = '@daily'
) as dag1:
    
    price_list = return_weatherdata()

    trigger_pipeline_continuation = TriggerDagRunOperator(
        task_id='trigger_pipeline_continuation',
        trigger_dag_id='dbt_project',  # Second DAG id
        wait_for_completion=True  # Set to True if you want the first DAG to wait for the second DAG's completion
    )

  # Task to process and load data
    process_data = create_load_incremental(price_list)

    price_list >> process_data>>trigger_pipeline_continuation

    



