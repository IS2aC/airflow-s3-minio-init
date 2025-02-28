""" Workflow for initialisation to sensor"""
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.dummy import DummyOperator
from datetime import datetime

BUCKET_NAME = "miniobucket"
FILE_NAME = "send_on_minio.txt" # Chemin du fichier dans MinIO

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 22),
    "retries": 1
}

with DAG(
    dag_id="s3_sensor_test",
    default_args=default_args,
    schedule_interval=None,  # Exécution manuelle
    catchup=False
) as dag:

    start = DummyOperator(task_id="start")

    wait_for_file = S3KeySensor(
        task_id="wait_for_s3_file",
        bucket_name=BUCKET_NAME,
        bucket_key=FILE_NAME,
        aws_conn_id="minio_default",
        poke_interval=10,  # Vérifie toutes les 10 secondes
        timeout=600,  # Timeout après 10 minutes
    )

    file_detected = DummyOperator(task_id="file_detected")

    start >> wait_for_file >> file_detected
