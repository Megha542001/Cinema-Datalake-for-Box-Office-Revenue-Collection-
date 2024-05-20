from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 6, 10),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def check_omdb_api():
    # Make a request to the OMDB API
    url = 'http://www.omdbapi.com/'
    params = {
        'apikey': '2f33835b',  # Replace with your OMDB API key
        't': 'Titanic'  # Replace with the movie title you want to search for
    }
    response = requests.get(url, params=params)
    
    # Check the response status code
    if response.status_code == 200:
        movie_data = response.json()
        # Process the movie data here
        print(movie_data)
    else:
        print(f"Request failed with status code: {response.status_code}")

with DAG('OMDB_API_Check', default_args=default_args, schedule_interval='@daily') as dag:
    check_omdb_api_task = PythonOperator(
        task_id='check_omdb_api',
        python_callable=check_omdb_api
    )

    check_omdb_api_task
