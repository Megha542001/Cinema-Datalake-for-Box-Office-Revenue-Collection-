from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

def upload_file_to_bucket():
    # Specify your project ID
    project_id = 'singular-vector-389317'

    # Create a storage client with the project ID
    client = storage.Client(project=project_id)

    # Get the bucket
    bucket_name = 'movies_db'
    bucket = client.get_bucket(bucket_name)

    # Specify the file path and name
    file_path = '/Users/abishek/datalake/combined.parquet'
    blob_name = 'combined.parquet'

    # Create a blob (object) in the bucket
    blob = bucket.blob(blob_name)

    # Upload the file to the blob
    with open(file_path, 'rb') as f:
        blob.upload_from_file(f)

    print(f"File uploaded to bucket: {bucket.name}, Blob: {blob_name}")


default_args = {
    'owner': 'Abishek Thamizharasan',
    'start_date': datetime(2023, 6, 9),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG('upload_file_to_bucket_dag', default_args=default_args, schedule_interval='@daily') as dag:
    upload_task = PythonOperator(
        task_id='upload_file_to_bucket',
        python_callable=upload_file_to_bucket
    )

    upload_task
