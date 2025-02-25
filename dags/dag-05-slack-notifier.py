from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from slack_sdk import WebClient

def send_slack_message():
    """
    Простая функция для отправки сообщения в Slack
    """
    # Инициализация Slack клиента
    slack_token = "xoxe.xoxp-1-Mi0yLTg1MDUxMzMzMDU0MTEtODUwNTEzMzM1MTU3MS04NDk3OTA5OTUyMzkwLTg1MDAyMDk0ODg0NzAtOGRjN2Y2YjllZWQ0YmM2YjlkMDgyZDQyOWQ2ZDY4YTMzMDcyYzgzMThhZWE0MWRjYThiNjMyMjg4MjdkOThlNQ"
    slack_channel = "#alerts"
    
    client = WebClient(token=slack_token)
    
    # Создание сообщения
    message = f"""
    🚀 *Тестовое сообщение от Airflow DAG*
    
    Привет! Это тестовое сообщение, отправленное из Airflow DAG.
    
    • Время отправки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    • DAG ID: simple_slack_notifier
    • Сервер: {datetime.now().strftime('%Y%m%d')}
    
    Если вы видите это сообщение, значит, настройка Slack уведомлений работает корректно! 👍
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
    description='Простой DAG для отправки сообщений в Slack',
    schedule_interval='@once',  # Запускается только один раз при активации
    catchup=False
)

# Задача для отправки сообщения
send_message_task = PythonOperator(
    task_id='send_slack_message',
    python_callable=send_slack_message,
    dag=dag
)
