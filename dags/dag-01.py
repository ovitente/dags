from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import hashlib
import random
import string
import time

# Функция для вычисления MD5

def compute_md5():
    start_time = time.time()
    duration = 300  # Вычисления будут идти 5 минут

    while time.time() - start_time < duration:
        # Генерация случайной строки длиной 256 символов
        random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=256))
        # Вычисление MD5
        hashlib.md5(random_string.encode()).hexdigest()


# Аргументы по умолчанию
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создание DAG
with DAG(
    dag_id='md5_compute_dag',
    description='DAG для нагрузки на систему за счет вычисления MD5',
    schedule_interval=None,
    start_date=datetime(2024, 6, 1),
    catchup=False,
    default_args=default_args,
) as dag:

    # Создание параллельных задач
    tasks = []
    for i in range(20):  # Количество параллельных задач
        task = PythonOperator(
            task_id=f'compute_md5_task_{i}',
            python_callable=compute_md5
        )
        tasks.append(task)
