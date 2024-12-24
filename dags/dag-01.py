from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

# Длительное выполнение задачи (5 минут)
def long_running_task(task_id):
    print(f"Задача {task_id} началась.")
    # Нагрузка в цикле с паузами для имитации длительной работы
    start_time = time.time()
    while time.time() - start_time < 300:  # 300 секунд = 5 минут
        # Имитируем тяжелую вычислительную задачу
        result = 0
        for i in range(1, 10**6):  # Множество итераций для вычислений
            result += i
        # Добавляем искусственную задержку для более равномерной нагрузки
        time.sleep(0.1)  # Задержка 100 мс, чтобы нагрузка не была слишком резкой
    print(f"Задача {task_id} завершена.")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Инициализация DAG
with DAG(
    dag_id='long_running_heavy_load_dag',
    description='DAG для длительного нагрузочного тестирования (5 минут)',
    schedule_interval='@daily',
    start_date=datetime(2024, 6, 1),
    catchup=False,
    default_args=default_args,
    concurrency=10,  # Параллельное выполнение до 5 задач одновременно
    max_active_runs=3,  # Ограничение на одновременные запуски DAG
) as dag:

    # Создаем 10 задач с длительным временем работы
    tasks = []
    for i in range(20):  # 10 задач
        task = PythonOperator(
            task_id=f'long_running_task_{i}',
            python_callable=long_running_task,
            op_args=[f'task_{i}'],
            task_concurrency=1,  # Каждая задача выполняется поочередно
        )
        tasks.append(task)

    # Задачи будут выполняться параллельно
    for task in tasks:
        task
