from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from functions.realtime import ingest_clickstream

from datetime import datetime

default_args = {
    'owner': 'ikigami',
}


with DAG(
    dag_id="2-ecommerce_clickstream_realtime_ingest",
    default_args=default_args,
    description="Realtime ingestion of e-commerce clickstream and user behavior",
    start_date=datetime(2025, 11, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["realtime", "ecommerce", "clickstream"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_clickstream",
        python_callable=ingest_clickstream,
    )

    ingest_task
