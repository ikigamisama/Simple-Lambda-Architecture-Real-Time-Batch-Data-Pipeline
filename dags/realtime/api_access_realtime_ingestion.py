from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from functions.realtime import ingest_api_logs

from datetime import datetime

default_args = {
    'owner': 'ikigami',
}


with DAG(
    dag_id="2-api_logs_realtime_ingest",
    default_args=default_args,
    description="Realtime ingestion of REST API access logs",
    start_date=datetime(2025, 11, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["realtime", "api", "logs"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_api_logs",
        python_callable=ingest_api_logs,
    )

    ingest_task
