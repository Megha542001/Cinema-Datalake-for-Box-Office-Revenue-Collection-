from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Abishek Thamizharasan',
    'start_date': datetime(2023, 6, 9),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG('cinema_collection_dag', default_args=default_args, schedule_interval='@daily') as dag:
    run_movie_script = BashOperator(
        task_id='run_movie_script',
        bash_command='python /Users/abishek/airflow/dags/scripts/data_collection.py',
        dag=dag
    )
