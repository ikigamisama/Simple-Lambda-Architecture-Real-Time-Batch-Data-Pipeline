from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from functions.batch import bronze_layer, silver_layer, gold_layer
from datetime import datetime

default_args = {
    'owner': 'ikigami',
}


with DAG(
    '3-batch_etl_all_streams',
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    description='Batch ETL processing all 5 realtime streams: Bronze → Silver → Gold',
    schedule=None,
    catchup=False,
    tags=["batch", "etl", "lambda-architecture"],
) as dag:
    start = EmptyOperator(task_id="start")

    t_bronze = PythonOperator(
        task_id="bronze_layer",
        python_callable=bronze_layer,
    )

    t_silver = PythonOperator(
        task_id="silver_layer",
        python_callable=silver_layer,
    )

    t_gold = PythonOperator(
        task_id="gold_layer",
        python_callable=gold_layer,
    )

    end = EmptyOperator(task_id="end")

    start >> t_bronze >> t_silver >> t_gold >> end
