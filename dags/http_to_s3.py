""" workflow ETL to push http result on s3 bucket """
from airflow import DAG
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
from datetime import datetime

S3_BUCKET = "datalog"  # Remplacement de BUCKET_NAME
S3_KEY = "got.json"
URL = "https://thronesapi.com/api/v2/Characters"

with DAG(
    dag_id="got_api_to_s3",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    download_got_data = HttpToS3Operator(
        task_id="download_got_data",
        http_conn_id=None,  # Pas de connexion nécessaire
        endpoint=URL,  # URL complète
        method="GET",
        s3_bucket=S3_BUCKET,  # ✅ Correction ici
        s3_key=S3_KEY,
        aws_conn_id="minio_default",  # Connexion MinIO
        replace=True
    )

    download_got_data
