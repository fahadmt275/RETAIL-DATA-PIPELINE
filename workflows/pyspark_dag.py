# import all modules
import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocSubmitJobOperator,
)

# define the variables
PROJECT_ID = "singular-node-473906-k7"
REGION = "us-east1"
CLUSTER_NAME = "my-demo-cluster"
COMPOSER_BUCKET = "us-central1-demo-instance-53f34672-bucket"

GCS_JOB_FILE = f"gs://{COMPOSER_BUCKET}/data/INGESTION/retailerMysqlToLanding.py"
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE},
}

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

# define the dag
with DAG(
        dag_id="pyspark_dag",
        schedule_interval=None,
        description="DAG to start a Dataproc cluster, run PySpark job, and stop the cluster",
        default_args=ARGS,
        tags=["pyspark", "dataproc", "etl"]
) as dag:
    # define the Tasks
    start_cluster = DataprocStartClusterOperator(
        task_id="start_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    stop_cluster = DataprocStopClusterOperator(
        task_id="stop_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

# define the task dependencies
start_cluster >> pyspark_task >> stop_cluster