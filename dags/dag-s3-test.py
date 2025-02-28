from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3

def test_s3_access():
    s3 = boto3.client('s3')
    try:
        # Попытка получить список объектов в бакете
        response = s3.list_objects_v2(Bucket='prod-dags-logs')
        print(f"S3 access successful. Objects: {response.get('Contents', [])}")
        
        # Попытка создать тестовый файл
        s3.put_object(
            Bucket='prod-dags-logs',
            Key='test-log.txt',
            Body='This is a test log file'
        )
        print("Successfully created test file in S3")
    except Exception as e:
        print(f"Error accessing S3: {e}")

with DAG(
    'test_s3_access',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
) as dag:
    
    task = PythonOperator(
        task_id='test_s3_access',
        python_callable=test_s3_access,
    )
