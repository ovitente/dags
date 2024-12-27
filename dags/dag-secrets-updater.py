from airflow.decorators import dag, task
from airflow.models import Variable
from google.cloud import secretmanager
from pendulum import datetime
import os

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["Google Secret Manager", "Airflow Variables", "TaskFlow"],
)
def fetch_secret_from_gsm():
    @task
    def get_secret_from_gsm(secret_name: str):
        """Извлекает значение секрета из Google Secret Manager."""
        # Проверяем наличие файла сервисного аккаунта
        service_account_file = os.getenv("GSM_ACCESS_SERVICE_ACCOUNT")
        if not service_account_file or not os.path.isfile(service_account_file):
            raise FileNotFoundError("Сервисный аккаунт не найден. Проверьте переменную GSM_ACCESS_SERVICE_ACCOUNT.")

        # Устанавливаем путь к файлу сервисного аккаунта
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_file

        # Создаём клиент для Google Secret Manager
        client = secretmanager.SecretManagerServiceClient()

        # Полное имя секрета
        project_id = service_account_file.split("/")[-2]  # Извлекаем проект из пути к файлу
        secret_path = f"projects/master-booster-446012-g9/secrets/app1/versions/latest"

        # Получаем секрет
        response = client.access_secret_version(request={"name": secret_path})
        secret_value = response.payload.data.decode("UTF-8")

        return secret_value

    @task
    def set_airflow_variable(variable_name: str, value: str):
        """Сохраняет значение секрета в переменной Airflow."""
        Variable.set(variable_name, value)

    # Получаем секрет из GSM
    secret_value = get_secret_from_gsm("gitub-airflow-secrets-changer")
    # Сохраняем секрет в переменной Airflow
    set_airflow_variable("THE_SECRET", secret_value)

fetch_secret_from_gsm()

