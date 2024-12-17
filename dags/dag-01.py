
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Параметры DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Инициализация DAG
with DAG(
    dag_id='example_dag',
    description='Пример DAG для демонстрации работы Airflow',
    schedule_interval='@daily',  # DAG запускается раз в сутки
    start_date=datetime(2024, 6, 1),
    catchup=False,  # Отключить запуск DAG для прошедших дат
    default_args=default_args,
) as dag:

    # Функция для PythonOperator
    def print_hello_world():
        print("Hello, World! DAG успешно запустился.")

    # PythonOperator - задача с вызовом функции
    hello_task = PythonOperator(
        task_id='print_hello_world_task',
        python_callable=print_hello_world
    )

    # Зависимости задач
    hello_task
