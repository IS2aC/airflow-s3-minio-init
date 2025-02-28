""" worflow to count the number of buckets on MinIO using S3HOOK """

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

def list_buckets():
    hook = S3Hook(aws_conn_id="minio_default")  # Connexion définie dans Airflow
    client = hook.get_conn()  # Récupération du client boto3
    response = client.list_buckets()
    buckets =  response.get("Buckets", [])
    buckets =  [{'BucketName':i['Name']} for i in buckets]
    print(f"Buckets trouvés : {buckets}")


def count_buckets():
    """Compte le nombre de buckets sur MinIO"""
    hook = S3Hook(aws_conn_id="minio_default")  # Connexion définie dans Airflow
    client = hook.get_conn()  # Récupération du client boto3
    response = client.list_buckets()
    
    buckets = response.get("Buckets", [])  # Extraction des buckets
    bucket_count = len(buckets)

    print(f"Nombre total de buckets : {bucket_count}")
    return bucket_count

# Définition du DAG
with DAG(
    dag_id="count_minio_buckets",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    list_task =  PythonOperator(
        task_id = "list_buckets_task",
        python_callable =  list_buckets
    )

    count_task = PythonOperator(
        task_id="count_buckets_task",
        python_callable=count_buckets
    )

    list_task >> count_task
