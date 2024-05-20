from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Abishek Thamizharasan',
    'start_date': datetime(2023, 6, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('cinema_data_formatting_dag',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    run_python_task = BashOperator(
        task_id='run_python_file',
        bash_command='python /Users/abishek/airflow/dags/scripts/data_transformation.py',
        dag=dag
    )

run_python_task
