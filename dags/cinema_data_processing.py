from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Abishek Thamizharasan',
    'start_date': datetime(2023, 6, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('cinema_data_processing_dag',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    load_data_task = BashOperator(
        task_id='load_data',
        bash_command='python /Users/abishek/airflow/dags/scripts/data_loading.py',
        dag=dag
    )

    index_data_task = BashOperator(
        task_id='index_data',
        bash_command='python /Users/abishek/airflow/dags/scripts/data_indexing.py',
        dag=dag
    )

load_data_task >> index_data_task
