from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from functions.realtime import ingest_social_media

from datetime import datetime

default_args = {
    'owner': 'ikigami',
}


with DAG(
    dag_id="2-social_media_realtime_ingest",
    default_args=default_args,
    description="Realtime ingestion of social media engagement events",
    start_date=datetime(2025, 11, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["realtime", "social", "engagement"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_social_media",
        python_callable=ingest_social_media,
    )

    ingest_task
