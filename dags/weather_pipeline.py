from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import requests
import json

# Longitude and Latitude of lagos
LONGITUDE = '3.3947'
LATITUDE = '6.4541'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id='weather_etl_pipeline',
        default_args=default_args,
        schedule_interval='@daily',
        description='''An ETl Pipeline to Extract the Weather information of Lagos, Transform the data and Load it into
                     Postgres database''',
        catchup=False) as dags:
    @task
    def extract_weather_data():
        # Use HTTP Hook to get connection details

        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        # Build an API endpoint
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        # Make the request via the HTTP Hook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            logging.info("Successfully fetched weather data")
            return response.json()
        else:
            logging.info("Failed to fetch weather data")


    @task
    def transform_weather_data(weather_data):
        logging.info("Transforming weather data")
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data


    @task
    def load_weather_data(transformed_data):
        logging.info("creating Postgres connection")
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        logging.info("Creating table")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
             latitude FLOAT,
             longitude FLOAT,
             temperature FLOAT,
             windspeed FLOAT,
             winddirection FLOAT,
             weathercode INT,
             timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP   
        );
         """)

        logging.info("Inserting transformed data into the table")
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
         VALUES (%s, %s, %s, %s, %s, %s)
         """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()


    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
