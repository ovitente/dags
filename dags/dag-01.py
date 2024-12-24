from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Функция для нагрузки
def cpu_intensive_task(task_id):
    print(f"Запуск задачи {task_id}")
    # Создание вычислительной нагрузки
    result = 0
    for i in range(1, 10**7):
        result += i
    print(f"Задача {task_id} завершена с результатом {result}")

# init
with DAG(
    dag_id='heavy_load_dag',
    description='DAG для нагрузочного тестирования',
    schedule_interval='@daily',
    start_date=datetime(2024, 6, 1),
    catchup=False,
    default_args=default_args,
    concurrency=10,  # Ограничение на количество задач, которые могут одновременно выполняться в рамках DAG
    max_active_runs=3,  # Ограничение на количество одновременно выполняющихся DAG
) as dag:

    # Создание 50 задач для параллельных вычислений
    tasks = []
    for i in range(50):
        task = PythonOperator(
            task_id=f'cpu_intensive_task_{i}',
            python_callable=cpu_intensive_task,
            op_args=[f'task_{i}'],
            task_concurrency=5,  # Параллельные вычисления для каждой задачи
        )
        tasks.append(task)

    # Задачи выполняются параллельно
    for task in tasks:
        task
