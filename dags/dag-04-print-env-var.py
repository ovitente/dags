from airflow.decorators import dag, task
from pendulum import datetime
import os

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["environment variables", "example"],
)
def print_all_env_vars():
    @task
    def print_env_vars():
        """Выводит все переменные окружения, соответствующие JSON ключам."""
        # Ключи из предоставленного JSON
        env_vars = [
            "al_creative_db_id",
            "applovin_api_key",
            "applovin_cm_api_client_id",
            "applovin_cm_api_client_secret",
            "apps_notion_page_id",
            "appsflyer_cohortapi_key",
            "bb_table",
            "clickhouse",
            "debug",
            "default_bids_spreadsheetId",
            "email_search_settings",
            "gd_file_users_access",
            "gd_folder_id",
            "global_settings_notion_page_id",
            "google_token",
            "harvest_slack_chat_id",
            "ironsource_refreshtoken",
            "ironsource_secretkey",
            "mintergral_api_access_key",
            "mintergral_api_token",
            "mongo_db_link",
            "notion_almost_positive_bidder_db",
            "notion_apps_db_id",
            "notion_bidder_block_db",
            "notion_db_id",
            "notion_logs_db_id",
            "notion_pnl_db_id",
            "notion_positive_bidder_db",
            "notion_settings_db_id",
            "notion_token",
            "notion_transactions_db_id",
            "pnl_project_mongo_table",
            "pymongo",
            "s3_report_access_key",
            "s3_report_access_key_secret",
            "s3_report_url",
            "slack_api_token",
            "slack_chat_bidder",
            "slack_chat_creative_tests",
            "slack_chat_id",
            "slack_info_chat_id",
            "slack_token",
            "slack_warnings_chat_id",
            "spreadsheet_settings_id",
            "unity_api_token",
        ]

        print("FOUND SECRETS")
        for var in env_vars:
            value = os.getenv(var, f"Переменная {var} не задана")
            print(f"{var} = {value}")

    print_env_vars()

print_all_env_vars()

