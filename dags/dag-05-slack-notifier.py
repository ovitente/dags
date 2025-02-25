from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagBag, DagModel
from airflow.utils.db import create_session
from airflow.configuration import conf
from slack_sdk import WebClient
import os
import json
import subprocess
from datetime import datetime, timedelta

# Полный класс DagUpdateMonitor без использования GitPython
class DagUpdateMonitor:
    def __init__(self, slack_token, channel, dag_folder):
        """
        Initialize the DAG update monitor
        
        Args:
            slack_token (str): Slack bot token
            channel (str): Slack channel to send notifications to
            dag_folder (str): Path to the DAG folder
        """
        self.slack_client = WebClient(token=slack_token)
        self.channel = channel
        self.dag_folder = dag_folder
        self.state_file = '/opt/airflow/last_commit.json'
        
    def get_last_commit(self):
        """Get the last processed commit hash"""
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                return json.load(f)['last_commit']
        return None
        
    def save_last_commit(self, commit_hash):
        """Save the last processed commit hash"""
        with open(self.state_file, 'w') as f:
            json.dump({'last_commit': commit_hash}, f)
            
    def run_git_command(self, command):
        """
        Run git command and return output
        """
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            cwd=self.dag_folder
        )
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            print(f"Git command failed: {stderr.decode('utf-8')}")
            return None
        return stdout.decode('utf-8').strip()
            
    def get_changed_dags(self, old_commit, new_commit):
        """
        Get list of DAGs that were changed between commits using git command
        """
        changed_files = []
        
        if old_commit:
            output = self.run_git_command(f"git diff --name-only {old_commit} {new_commit}")
            if output:
                changed_files = [f for f in output.split('\n') if f.endswith('.py')]
        else:
            # First run - consider all .py files as changed
            changed_files = [f for f in os.listdir(self.dag_folder) if f.endswith('.py')]
            
        return changed_files
        
    def get_current_commit(self):
        """Get current commit hash"""
        return self.run_git_command("git rev-parse HEAD")
        
    def load_dags(self):
        """Load DAGs and return DAG objects"""
        dagbag = DagBag(self.dag_folder)
        if dagbag.import_errors:
            self.notify_error(dagbag.import_errors)
            return None
        return dagbag.dags
        
    def notify_update(self, changed_files, new_dags):
        """
        Send notification to Slack about DAG updates
        """
        message = f"🔄 *DAG обновления обнаружены*\n"
        message += f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        message += "*Измененные файлы:*\n"
        for file in changed_files:
            message += f"• {file}\n"
            
        message += "\n*Обновленные DAG'и:*\n"
        for dag_id in new_dags:
            dag = new_dags[dag_id]
            message += f"• {dag_id}\n"
            message += f"  - График: {dag.schedule_interval}\n"
            message += f"  - Владелец: {dag.owner}\n"
            
        self.slack_client.chat_postMessage(
            channel=self.channel,
            text=message,
            mrkdwn=True
        )
        
    def notify_error(self, errors):
        """
        Send notification about import errors
        """
        message = "❌ *Ошибки при импорте DAG'ов*\n\n"
        for filename, error in errors.items():
            message += f"*Файл:* {filename}\n"
            message += f"*Ошибка:* ```{error}```\n\n"
            
        self.slack_client.chat_postMessage(
            channel=self.channel,
            text=message,
            mrkdwn=True
        )
        
    def check_updates(self):
        """
        Main method to check for updates and send notifications
        """
        current_commit = self.get_current_commit()
        if not current_commit:
            print("Failed to get current commit hash")
            return
            
        last_commit = self.get_last_commit()
        
        if current_commit != last_commit:
            changed_files = self.get_changed_dags(last_commit, current_commit)
            if changed_files:
                new_dags = self.load_dags()
                if new_dags:
                    self.notify_update(changed_files, new_dags)
                    self.save_last_commit(current_commit)

# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'monitor_dag_updates',
    default_args=default_args,
    schedule_interval='*/10 * * * *',  # Проверка каждые 10 минут
    catchup=False
)

# Создаём оператор
check_updates = PythonOperator(
    task_id='check_dag_updates',
    python_callable=DagUpdateMonitor(
        slack_token="ваш-токен",
        channel="#ваш-канал",
        dag_folder="/opt/airflow/dags/repo/dags"
    ).check_updates,
    dag=dag
)
