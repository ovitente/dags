
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# init
with DAG(
    dag_id='hello_moto',
    description='Пример DAG для демонстрации работы Airflow',
    schedule_interval='@daily',
    start_date=datetime(2024, 6, 1),
    catchup=False,
    default_args=default_args,
) as dag:

    def print_hello_world():
        print("Hello, MOTO! DAG успешно запустился.")

    hello_task = PythonOperator(
        task_id='print_hello_world_task',
        python_callable=print_hello_world
    )

    hello_task
