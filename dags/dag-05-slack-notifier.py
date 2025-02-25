from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'monitor_dag_updates',
    default_args=default_args,
    schedule_interval='*/10 * * * *',  
    catchup=False
)

check_updates = PythonOperator(
    task_id='check_dag_updates',
    python_callable=DagUpdateMonitor(
        slack_token="xoxe.xoxp-1-Mi0yLTg1MDUxMzMzMDU0MTEtODUwNTEzMzM1MTU3MS04NDk3OTA5OTUyMzkwLTg0OTc5MDk5ODQ2OTQtNDJhNzFmNTgzY2E0ZGI2M2NiY2IzYWYwNGI4ZmFiODI2MjdhMWQwYzNlYTc2ZDZmZmQ0OGZmOGNjNmEyZjQ3NQ",
        channel="#alerts",
        dag_folder="/opt/airflow/dags"
    ).check_updates,
    dag=dag
)
