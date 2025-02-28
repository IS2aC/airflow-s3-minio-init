"""Workflow to copy a file from one S3 bucket to another S3 bucket."""

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from datetime import datetime

FIRST_BUCKET = "datacatalog"
S3_KEY_FIRST_BUCKET = "got.json"
SECOND_BUCKET = "datalake"
S3_KEY_SECOND_BUCKET = "copied_got.json"


with DAG(
    dag_id="s3_to_s3",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    transfer_file = S3CopyObjectOperator(
        task_id="transfer_file",
        source_bucket_name=FIRST_BUCKET,
        source_bucket_key=S3_KEY_FIRST_BUCKET,
        dest_bucket_name=SECOND_BUCKET,
        dest_bucket_key=S3_KEY_SECOND_BUCKET,
        aws_conn_id="minio_default",
    )

    transfer_file