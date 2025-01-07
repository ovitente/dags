from airflow.decorators import dag, task
from pendulum import datetime
import os

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["environment variable", "example"],
)
def print_env_var():
    @task
    def print_unicorn_env():
        """Выводит значение переменной окружения UNICORN."""
        unicorn_value = os.getenv("UNICORN", "Переменная UNICORN не задана")
        print(f"Значение переменной UNICORN: {unicorn_value}")

    print_unicorn_env()

print_env_var()

