# plugins/dag_update_listener.py
from airflow.listeners.listener import hook
from airflow.models.dag import DagModel
from airflow.models import DagBag
from slack_sdk import WebClient
import os
import json
import hashlib
from airflow.models import Variable
from airflow.utils.timezone import utcnow

class DAGUpdateListener:
    """
    Listener, который отслеживает обновления DAG'ов и отправляет уведомления в Slack
    """
    
    def __init__(self):
        # Получаем токен Slack из переменных окружения
        self.slack_token = os.environ.get('SLACK_TOKEN')
        self.slack_channel = os.environ.get('SLACK_CHANNEL', '#alerts')
        
        # Имя переменной для хранения хешей файлов DAG
        self.hash_var_name = 'dag_file_hashes'
        
        # Инициализируем клиент Slack, если токен доступен
        if self.slack_token:
            self.slack_client = WebClient(token=self.slack_token)
        else:
            print("WARNING: No Slack token provided. Notifications will not be sent.")
            self.slack_client = None

    def compute_file_hash(self, file_path):
        """
        Вычисляет MD5 хеш содержимого файла
        """
        try:
            with open(file_path, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except Exception as e:
            print(f"Error computing hash for {file_path}: {e}")
            return None
    
    def get_dag_metadata(self, dag_id):
        """
        Получает метаданные для конкретного DAG из базы данных Airflow
        """
        from airflow.utils.db import create_session
        
        with create_session() as session:
            dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
            if dag_model:
                return {
                    'owners': dag_model.owners,
                    'schedule_interval': dag_model.schedule_interval,
                    'last_parsed_time': dag_model.last_parsed_time
                }
        return None
    
    def notify_slack(self, dag_id, event_type):
        """
        Отправляет уведомление в Slack о событии DAG
        """
        if not self.slack_client:
            print("Slack client not initialized. Skipping notification.")
            return
        
        # Получаем метаданные DAG
        metadata = self.get_dag_metadata(dag_id)
        if not metadata:
            print(f"No metadata found for DAG {dag_id}")
            return
        
        # Получаем информацию о файле DAG
        dagbag = DagBag()
        dag = dagbag.get_dag(dag_id)
        file_path = dag.fileloc if dag else "Unknown"
        
        # Создаем сообщение
        if event_type == "added":
            emoji = "➕"
            title = "Новый DAG добавлен"
        elif event_type == "updated":
            emoji = "🔄"
            title = "DAG обновлен"
        else:
            emoji = "🔔"
            title = "Событие DAG"
        
        message = f"{emoji} *{title}*\n"
        message += f"*DAG ID:* {dag_id}\n"
        message += f"*Время:* {utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
        message += f"*Файл:* `{os.path.basename(file_path)}`\n"
        message += f"*Расписание:* `{metadata['schedule_interval'] or 'None'}`\n"
        message += f"*Владелец:* {metadata['owners'] or 'None'}\n"
        
        # Отправляем сообщение
        try:
            response = self.slack_client.chat_postMessage(
                channel=self.slack_channel,
                text=message,
                mrkdwn=True
            )
            print(f"Slack notification sent successfully: {response['ts']}")
        except Exception as e:
            print(f"Error sending Slack notification: {e}")


# Инициализируем слушатель
dag_listener = DAGUpdateListener()

@hook('dag')
def dag_modified_listener(event_type, dag_id, *args, **kwargs):
    """
    Хук, который вызывается при модификации DAG'а
    
    Поддерживаемые event_type: 'dag_created', 'dag_modified'
    """
    # Пропускаем текущий мониторинговый DAG
    if dag_id == "monitor_dag_updates":
        return
    
    print(f"DAG event: {event_type} for DAG {dag_id}")
    
    # Определяем тип события
    if event_type == 'dag_created':
        dag_listener.notify_slack(dag_id, "added")
    elif event_type == 'dag_modified':
        dag_listener.notify_slack(dag_id, "updated")
