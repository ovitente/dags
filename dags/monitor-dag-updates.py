from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagModel, DagBag, Variable
from airflow.utils.db import create_session
from airflow.utils.state import State
from airflow.utils.timezone import utcnow, make_aware
from slack_sdk import WebClient
import os
from datetime import datetime, timedelta

class DAGUpdateDBMonitor:
    """
    A class to monitor DAG updates using Airflow's database.
    This approach doesn't require storing state in files and works reliably in Kubernetes.
    """
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
        
        # Get the last check time from Airflow Variables or use default
        try:
            last_check_str = Variable.get('dag_monitor_last_check_time')
            self.last_check_time = make_aware(datetime.fromisoformat(last_check_str))
            print(f"Retrieved last check time from Variable: {self.last_check_time}")
        except (KeyError, ValueError):
            # Default to 10 minutes ago if no previous record exists
            self.last_check_time = utcnow() - timedelta(minutes=10)
            print(f"No previous check time found, using default: {self.last_check_time}")
        
        # Initialize Slack client if token is available
        if self.slack_token:
            self.slack_client = WebClient(token=self.slack_token)
        else:
            print("WARNING: No Slack token provided. Notifications will not be sent.")
            self.slack_client = None
    
    def get_updated_dags(self):
        """
        Get DAGs that were updated since the last check.
        Uses Airflow's database to determine what changed.
        
        Returns:
            list: List of updated DAG objects with metadata
        """
        updated_dags = []
        
        # Query the database for recently updated DAGs
        with create_session() as session:
            query = session.query(DagModel).filter(
                DagModel.last_parsed_time > self.last_check_time
            )
            
            for dag_model in query.all():
                updated_dags.append({
                    'dag_id': dag_model.dag_id,
                    'fileloc': dag_model.fileloc,
                    'last_parsed_time': dag_model.last_parsed_time,
                    'owners': dag_model.owners,
                    'schedule_interval': dag_model.schedule_interval
                })
        
        return updated_dags
    
    def get_removed_dags(self):
        """
        Get DAGs that were removed from the system.
        
        Returns:
            list: List of removed DAG IDs
        """
        removed_dags = []
        
        with create_session() as session:
            # Find DAGs that are no longer present in the files
            query = session.query(DagModel).filter(
                DagModel.is_active == False,
                DagModel.is_subdag == False
            )
            
            for dag_model in query.all():
                # Only include if it was active before our last check
                # and is now marked as inactive
                if dag_model.last_parsed_time and dag_model.last_parsed_time > self.last_check_time:
                    removed_dags.append({
                        'dag_id': dag_model.dag_id,
                        'fileloc': dag_model.fileloc
                    })
        
        return removed_dags
    
    def notify_slack(self, updated_dags, removed_dags):
        """
        Send notification to Slack about DAG updates.
        
        Args:
            updated_dags (list): List of updated DAG objects
            removed_dags (list): List of removed DAG objects
        """
        if not self.slack_client:
            print("Slack client not initialized. Skipping notification.")
            return
            
        if not updated_dags and not removed_dags:
            print("No changes to report")
            return
        
        # Create the message
        message = f"🔄 *Обнаружены обновления DAG*\n"
        message += f"Время: {utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"
        
        # Add updated DAGs section
        if updated_dags:
            message += "*Обновленные DAG'и:*\n"
            for dag in updated_dags:
                # Get just the filename from the path
                file_name = os.path.basename(dag['fileloc'])
                
                message += f"• *{dag['dag_id']}*\n"
                message += f"  - Файл: `{file_name}`\n"
                message += f"  - Расписание: `{dag['schedule_interval'] or 'None'}`\n"
                message += f"  - Владелец: {dag['owners'] or 'None'}\n"
                message += f"  - Обновлен: {dag['last_parsed_time'].strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        # Add removed DAGs section
        if removed_dags:
            message += "\n*Удаленные DAG'и:*\n"
            for dag in removed_dags:
                message += f"• *{dag['dag_id']}*\n"
                message += f"  - Файл: `{os.path.basename(dag['fileloc'])}`\n"
        
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
        print(f"Checking for DAG updates since {self.last_check_time}...")
        
        # Store the current time to use in the next run
        current_time = utcnow()
        
        # Get updated and removed DAGs
        updated_dags = self.get_updated_dags()
        removed_dags = self.get_removed_dags()
        
        # Send notification if there are any changes
        if updated_dags or removed_dags:
            print(f"Found {len(updated_dags)} updated DAGs and {len(removed_dags)} removed DAGs")
            self.notify_slack(updated_dags, removed_dags)
        else:
            print("No changes detected")
        
        # Update the last check time for the next run and save to Variables
        self.last_check_time = current_time
        Variable.set('dag_monitor_last_check_time', current_time.isoformat())
        print(f"Saved current time to Variable: {current_time}")


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
    description='Monitor DAG updates using Airflow DB and send notifications to Slack',
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    catchup=False
)

# Create task to check for updates
monitor = DAGUpdateDBMonitor()
check_updates_task = PythonOperator(
    task_id='check_dag_updates',
    python_callable=monitor.check_updates,
    dag=dag
)
