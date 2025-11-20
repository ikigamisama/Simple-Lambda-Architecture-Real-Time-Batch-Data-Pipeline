from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator

from datetime import datetime

default_args = {
    'owner': 'ikigami',
}

with DAG(
    '1-infra_s3_bootstrap',
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    description='Medallion ETL for Innovation & Connectivity metrics from OWID',
    schedule=None,
    catchup=False,
    tags=['s3', 'lambda', 'architecture']
) as dag:
    bucket = "lambda-architecture"

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket",
        bucket_name=bucket,
        aws_conn_id="aws_default"
    )

    create_s3_bucket
