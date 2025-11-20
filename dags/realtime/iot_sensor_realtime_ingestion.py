from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from functions.realtime import ingest_iot_sensors

from datetime import datetime

default_args = {
    'owner': 'ikigami',
}


with DAG(
    dag_id="2-iot_sensors_realtime_ingest",
    default_args=default_args,
    description="Realtime ingestion of IoT sensor data from smart buildings",
    start_date=datetime(2025, 11, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["realtime", "iot", "sensors"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_iot_sensors",
        python_callable=ingest_iot_sensors,
    )

    ingest_task
