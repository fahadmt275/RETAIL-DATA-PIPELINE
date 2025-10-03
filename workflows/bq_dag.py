import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Define constants
PROJECT_ID = "singular-node-473906-k7"
LOCATION = "US"
SQL_FILE_PATH_1 = "/home/airflow/gcs/data/BQ/raw.sql"
SQL_FILE_PATH_2 = "/home/airflow/gcs/data/BQ/intermediate.sql"
SQL_FILE_PATH_3 = "/home/airflow/gcs/data/BQ/main.sql"

# Read SQL query from file
def read_sql_file(file_path):
    with open(file_path, "r") as file:
        return file.read()

RAW_QUERY = read_sql_file(SQL_FILE_PATH_1)
INTERMEDIATE_QUERY = read_sql_file(SQL_FILE_PATH_2)
MAIN_QUERY = read_sql_file(SQL_FILE_PATH_3)

# Define default arguments
ARGS = {
    "owner": "Fahad MT",
    "start_date": None,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["***@gmail.com"],
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Define the DAG
with DAG(
    dag_id="bigquery_dag",
    schedule_interval=None,
    description="DAG to run the bigquery jobs",
    default_args=ARGS,
    tags=["gcs", "bq", "etl"]
) as dag:

    # Task to create main_tables
    raw_tables = BigQueryInsertJobOperator(
        task_id="raw_tables",
        configuration={
            "query": {
                "query": RAW_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task to create main_tables
    intermediate_tables = BigQueryInsertJobOperator(
        task_id="intermediate_tables",
        configuration={
            "query": {
                "query": INTERMEDIATE_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task to create main_tables
    main_tables = BigQueryInsertJobOperator(
        task_id="main_tables",
        configuration={
            "query": {
                "query": MAIN_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

# Define dependencies
raw_tables >> intermediate_tables >> main_tables