from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from functions.realtime import ingest_financial_transactions

from datetime import datetime

default_args = {
    'owner': 'ikigami',
}


with DAG(
    dag_id="2-financial_transactions_realtime_ingest",
    default_args=default_args,
    description="Realtime ingestion of financial transactions with fraud detection",
    start_date=datetime(2025, 11, 1),
    schedule="*/5 * * * *",  # Every 3 minutes
    catchup=False,
    max_active_runs=1,
    tags=["realtime", "financial", "fraud-detection"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_financial_transactions",
        python_callable=ingest_financial_transactions,
    )

    ingest_task
