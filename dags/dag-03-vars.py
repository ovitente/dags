import os
from google.cloud import secretmanager

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
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

    # Получаем секрет
    response = client.access_secret_version(request={"name": secret_path})
    secret_value = response.payload.data.decode("UTF-8")

    return secret_value

if __name__ == "__main__":
    secret_name = "gitub-airflow-secrets-changer"
    secret = get_secret_from_gsm(secret_name)
    print(f"Секрет: {secret}")

