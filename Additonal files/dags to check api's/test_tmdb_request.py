from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import logging

def test_tmdb_request():
    # Fetch movie data from TMDB API
    tmdb_api_key = 'bfd1fa0b47958850ad4d9f1606f1c15a'
    tmdb_url = f'https://api.themoviedb.org/3/discover/movie?api_key={tmdb_api_key}'
    tmdb_response = requests.get(tmdb_url)
    
    # Log response status and content
    logging.info(f"TMDB Response Status Code: {tmdb_response.status_code}")
    logging.info(f"TMDB Response Content: {tmdb_response.json()}")

default_args = {
    'owner': 'Abishek Thamizharasan',
    'start_date': datetime(2023, 6, 11),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG('test_tmdb_request_dag', default_args=default_args, schedule_interval='@daily') as dag:
    test_tmdb_request_task = PythonOperator(
        task_id='test_tmdb_request',
        python_callable=test_tmdb_request
    )

