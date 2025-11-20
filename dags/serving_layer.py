from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from functions.serving import merge_iot_sensors, merge_api_logs, merge_social_media, merge_ecommerce, merge_financial, publish_analytics_views

from datetime import datetime

default_args = {
    'owner': 'ikigami',
}


with DAG(
    '4-serving_layer_merge',
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    description="Lambda Architecture Serving Layer - Merges batch and speed layers",
    schedule=None,
    catchup=False,
    tags=["serving", "lambda", "realtime-analytics"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Parallel merge tasks for each stream
    t_iot = PythonOperator(
        task_id="merge_iot_sensors",
        python_callable=merge_iot_sensors,
    )

    t_api = PythonOperator(
        task_id="merge_api_logs",
        python_callable=merge_api_logs,
    )

    t_social = PythonOperator(
        task_id="merge_social_media",
        python_callable=merge_social_media,
    )

    t_ecom = PythonOperator(
        task_id="merge_ecommerce",
        python_callable=merge_ecommerce,
    )

    t_financial = PythonOperator(
        task_id="merge_financial",
        python_callable=merge_financial,
    )

    t_publish = PythonOperator(
        task_id="publish_analytics_views",
        python_callable=publish_analytics_views,
    )

    end = EmptyOperator(task_id="end")

    start >> [t_iot, t_api, t_social, t_ecom, t_financial] >> t_publish >> end
