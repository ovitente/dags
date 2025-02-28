from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagModel, DagBag, Variable
from airflow.utils.db import create_session
from airflow.utils.timezone import utcnow, make_aware
from slack_sdk import WebClient
import os
import json
import hashlib
from datetime import datetime, timedelta

class DAGFileHashMonitor:
    def __init__(self, slack_token=None, slack_channel=None):
        """
        Initialize the DAG update monitor.
        
        Args:
            slack_token (str, optional): Slack bot token. If None, will try to get from env.
            slack_channel (str, optional): Slack channel to send notifications. If None, will try to get from env.
        """
        # Get credentials from environment if not provided
        self.slack_token = slack_token or os.environ.get('SLACK_TOKEN')
        self.slack_channel = slack_channel or os.environ.get('SLACK_CHANNEL', '#alerts')
        
        # Get DAG folder path from Airflow config
        from airflow.configuration import conf
        self.dag_folder = os.path.expanduser(conf.get('core', 'dags_folder'))
        
        # Variable name for storing DAG file hashes
        self.hash_var_name = 'dag_file_hashes'
        
        # Initialize Slack client if token is available
        if self.slack_token:
            self.slack_client = WebClient(token=self.slack_token)
        else:
            print("WARNING: No SLACK token provided. Notifications will not be sent.")
            self.slack_client = None
    
    def get_dag_files(self):
        """
        Get all Python files in the DAG folder.
        
        Returns:
            dict: Dictionary of {dag_id: file_path}
        """
        # Load all DAGs to get their IDs and file paths
        dagbag = DagBag(self.dag_folder)
        
        dag_files = {}
        for dag_id, dag in dagbag.dags.items():
            # Skip monitor DAG to avoid notification loop
            if dag_id == "monitor_dag_updates":
                continue
            dag_files[dag_id] = dag.fileloc
        
        return dag_files
    
    def compute_file_hash(self, file_path):
        """
        Compute MD5 hash of a file's contents.
        
        Args:
            file_path (str): Path to the file
            
        Returns:
            str: MD5 hash of the file contents
        """
        try:
            with open(file_path, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except Exception as e:
            print(f"Error computing hash for {file_path}: {e}")
            return None
    
    def get_current_hashes(self, dag_files):
        """
        Compute hashes for all DAG files.
        
        Args:
            dag_files (dict): Dictionary of {dag_id: file_path}
            
        Returns:
            dict: Dictionary of {dag_id: {file_path, hash}}
        """
        hashes = {}
        for dag_id, file_path in dag_files.items():
            file_hash = self.compute_file_hash(file_path)
            if file_hash:
                hashes[dag_id] = {
                    'file_path': file_path,
                    'hash': file_hash
                }
        return hashes
    
    def get_previous_hashes(self):
        """
        Get previously stored DAG file hashes from Airflow Variable.
        
        Returns:
            dict: Dictionary of {dag_id: {file_path, hash}}
        """
        try:
            hash_json = Variable.get(self.hash_var_name)
            return json.loads(hash_json)
        except (KeyError, ValueError, json.JSONDecodeError) as e:
            print(f"No previous hashes found or error parsing: {e}")
            return {}
    
    def save_current_hashes(self, hashes):
        """
        Save current DAG file hashes to Airflow Variable.
        
        Args:
            hashes (dict): Dictionary of {dag_id: {file_path, hash}}
        """
        try:
            Variable.set(self.hash_var_name, json.dumps(hashes))
            print(f"Saved {len(hashes)} file hashes to Variable '{self.hash_var_name}'")
        except Exception as e:
            print(f"Error saving hashes to Variable: {e}")
    
    def get_dag_metadata(self, dag_id):
        """
        Get metadata for a specific DAG from Airflow database.
        
        Args:
            dag_id (str): ID of the DAG
            
        Returns:
            dict: Metadata for the DAG
        """
        with create_session() as session:
            dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
            if dag_model:
                return {
                    'owners': dag_model.owners,
                    'schedule_interval': dag_model.schedule_interval,
                    'last_parsed_time': dag_model.last_parsed_time
                }
        return None
    
    def detect_changes(self, prev_hashes, curr_hashes):
        """
        Detect changes between previous and current hashes.
        
        Args:
            prev_hashes (dict): Previous hashes
            curr_hashes (dict): Current hashes
            
        Returns:
            tuple: (added_dags, modified_dags, removed_dags)
        """
        added_dags = []
        modified_dags = []
        removed_dags = []
        
        # Find added and modified DAGs
        for dag_id, data in curr_hashes.items():
            if dag_id not in prev_hashes:
                # New DAG
                added_dags.append(dag_id)
            elif data['hash'] != prev_hashes[dag_id]['hash']:
                # Modified DAG
                modified_dags.append(dag_id)
        
        # Find removed DAGs
        for dag_id in prev_hashes:
            if dag_id not in curr_hashes:
                removed_dags.append(dag_id)
        
        return added_dags, modified_dags, removed_dags
    
    def notify_slack(self, added_dags, modified_dags, removed_dags):
        """
        Send notification to Slack about DAG changes.
        
        Args:
            added_dags (list): List of new DAG IDs
            modified_dags (list): List of modified DAG IDs
            removed_dags (list): List of removed DAG IDs
        """
        if not self.slack_client:
            print("Slack client not initialized. Skipping notification.")
            return
        
        if not added_dags and not modified_dags and not removed_dags:
            print("No changes to report")
            return
        
        # Create the message
        message = f"🔄 *Обнаружены изменения в DAG'ах*\n"
        message += f"Время: {utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"
        
        # Added DAGs section
        if added_dags:
            message += "*Новые DAG'и:*\n"
            for dag_id in added_dags:
                metadata = self.get_dag_metadata(dag_id)
                if metadata:
                    message += f"• *{dag_id}*\n"
                    message += f"  - Файл: `{os.path.basename(curr_hashes[dag_id]['file_path'])}`\n"
                    message += f"  - Расписание: `{metadata['schedule_interval'] or 'None'}`\n"
                    message += f"  - Владелец: {metadata['owners'] or 'None'}\n"
        
        # Modified DAGs section
        if modified_dags:
            message += "\n*Измененные DAG'и:*\n"
            for dag_id in modified_dags:
                metadata = self.get_dag_metadata(dag_id)
                if metadata:
                    message += f"• *{dag_id}*\n"
                    message += f"  - Файл: `{os.path.basename(curr_hashes[dag_id]['file_path'])}`\n"
                    message += f"  - Расписание: `{metadata['schedule_interval'] or 'None'}`\n"
                    message += f"  - Владелец: {metadata['owners'] or 'None'}\n"
        
        # Removed DAGs section
        if removed_dags:
            message += "\n*Удаленные DAG'и:*\n"
            for dag_id in removed_dags:
                file_path = prev_hashes[dag_id]['file_path']
                message += f"• *{dag_id}*\n"
                message += f"  - Файл: `{os.path.basename(file_path)}`\n"
        
        # Send the message
        try:
            response = self.slack_client.chat_postMessage(
                channel=self.slack_channel,
                text=message,
                mrkdwn=True
            )
            print(f"Slack notification sent successfully: {response['ts']}")
        except Exception as e:
            print(f"Error sending Slack notification: {e}")
    
    def check_updates(self):
        """
        Main method to check for DAG updates and send notifications.
        """
        print(f"Checking for DAG updates in {self.dag_folder}...")
        
        # Get all DAG files
        dag_files = self.get_dag_files()
        print(f"Found {len(dag_files)} DAGs in the system")
        
        # Get previous file hashes
        global prev_hashes
        prev_hashes = self.get_previous_hashes()
        print(f"Loaded {len(prev_hashes)} previous file hashes")
        
        # Compute current file hashes
        global curr_hashes
        curr_hashes = self.get_current_hashes(dag_files)
        print(f"Computed {len(curr_hashes)} current file hashes")
        
        # Detect changes
        added_dags, modified_dags, removed_dags = self.detect_changes(prev_hashes, curr_hashes)
        
        # Send notification if there are changes
        if added_dags or modified_dags or removed_dags:
            print(f"Found {len(added_dags)} new DAGs, {len(modified_dags)} modified DAGs, and {len(removed_dags)} removed DAGs")
            self.notify_slack(added_dags, modified_dags, removed_dags)
        else:
            print("No changes detected in DAG files")
        
        # Save current hashes for next run
        self.save_current_hashes(curr_hashes)


# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 25, tzinfo=utcnow().tzinfo),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'monitor_dag_updates',
    default_args=default_args,
    description='Monitor DAG updates using file hashing and send notifications to Slack',
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    catchup=False
)

# Create task to check for updates
monitor = DAGFileHashMonitor()
check_updates_task = PythonOperator(
    task_id='check_dag_updates',
    python_callable=monitor.check_updates,
    dag=dag
)
