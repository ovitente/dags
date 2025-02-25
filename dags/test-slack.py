from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from slack_sdk import WebClient
import os

def send_slack_message():
    # Получение токена из переменной окружения
    slack_token = os.environ.get('SLACK_TOKEN')
    if not slack_token:
        raise ValueError("Переменная окружения SLACK_TOKEN не установлена")
    
    slack_channel = os.environ.get('SLACK_CHANNEL', '#alerts')
    
    client = WebClient(token=slack_token)
    
    # Создание сообщения
    message = f"""
    🚀 *Тестовое сообщение от Airflow DAG*
    
    Привет! Это тестовое сообщение, отправленное из Airflow DAG.
    
    • Время отправки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    • DAG ID: simple_slack_notifier
    • Сервер: {os.environ.get('HOSTNAME', 'неизвестно')}
    
    Если вы видите это сообщение, значит, настройка Slack уведомлений с использованием
    переменных окружения работает корректно! 👍
    """
    
    # Отправка сообщения
    try:
        response = client.chat_postMessage(
            channel=slack_channel,
            text=message,
            mrkdwn=True
        )
        print(f"Сообщение успешно отправлено: {response['ts']}")
        return True
    except Exception as e:
        print(f"Ошибка при отправке сообщения: {e}")
        return False

# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_slack_notifier',
    default_args=default_args,
    description='Простой DAG для отправки сообщений в Slack с использованием переменных окружения',
    schedule_interval='@once',  # Запускается только один раз при активации
    catchup=False
)

# Задача для отправки сообщения
send_message_task = PythonOperator(
    task_id='send_slack_message',
    python_callable=send_slack_message,
    dag=dag
)
