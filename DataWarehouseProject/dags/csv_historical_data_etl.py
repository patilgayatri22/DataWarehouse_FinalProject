from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_historyweather_data',
    default_args=default_args,
    description='Load weather data from CSV to Snowflake',
    schedule_interval=timedelta(days=1),
)

def process_csv():
    df = pd.read_csv('/Downloads/weather_data_california_historical.csv', skiprows=3)
    df['DATE'] = pd.to_datetime(df['datetime']).dt.date.astype(str)
    df['SUNRISE'] = pd.to_datetime(df['sunrise']).dt.time.astype(str)
    df['SUNSET'] = pd.to_datetime(df['sunset']).dt.time.astype(str)
    
    columns_to_keep = ['DATE', 'tempmax', 'tempmin', 'temp', 'feelslikemax', 'humidity', 'precip', 
                       'windspeed', 'cloudcover', 'conditions', 'SUNRISE', 'SUNSET', 'solarradiation']
    df = df[columns_to_keep]
    
    column_mapping = {
        'tempmax': 'TEMPMAX',
        'tempmin': 'TEMPMIN',
        'temp': 'TEMP',
        'feelslikemax': 'FEELSLIKE',
        'humidity': 'HUMIDITY',
        'precip': 'PRECIP',
        'windspeed': 'WINDSPEED',
        'cloudcover': 'CLOUDCOVER',
        'conditions': 'CONDITIONS',
        'solarradiation': 'SOLARRADIATION'
    }
    df = df.rename(columns=column_mapping)
    
    # Convert all columns to strings to ensure JSON serializability
    for col in df.columns:
        df[col] = df[col].astype(str)
    
    return df.to_dict('records')

def upload_to_snowflake(**context):
    records = context['task_instance'].xcom_pull(task_ids='process_csv')
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS WEATHER.DATA.HISTORICAL_DATA (
        DATE DATE NOT NULL,
        TEMPMAX FLOAT,
        TEMPMIN FLOAT,
        TEMP FLOAT,
        FEELSLIKE FLOAT,
        HUMIDITY FLOAT,
        PRECIP FLOAT,
        WINDSPEED FLOAT,
        CLOUDCOVER FLOAT,
        CONDITIONS VARCHAR(16777216),
        SUNRISE TIME(9),
        SUNSET TIME(9),
        SOLARRADIATION FLOAT,
        PRIMARY KEY (DATE)
    )
    """
    cursor.execute(create_table_query)
    
    insert_query = """
    INSERT INTO WEATHER.DATA.HISTORICAL_DATA (
        DATE, TEMPMAX, TEMPMIN, TEMP, FEELSLIKE, HUMIDITY, PRECIP, WINDSPEED, CLOUDCOVER, CONDITIONS, SUNRISE, SUNSET, SOLARRADIATION
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, [
        (r['DATE'], float(r['TEMPMAX']), float(r['TEMPMIN']), float(r['TEMP']), float(r['FEELSLIKE']), 
         float(r['HUMIDITY']), float(r['PRECIP']), float(r['WINDSPEED']), float(r['CLOUDCOVER']), 
         r['CONDITIONS'], r['SUNRISE'], r['SUNSET'], float(r['SOLARRADIATION']))
        for r in records
    ])
    conn.commit()
    cursor.close()
    conn.close()

process_csv_task = PythonOperator(
    task_id='process_csv',
    python_callable=process_csv,
    dag=dag,
)

upload_to_snowflake_task = PythonOperator(
    task_id='upload_to_snowflake',
    python_callable=upload_to_snowflake,
    provide_context=True,
    dag=dag,
)

process_csv_task >> upload_to_snowflake_task