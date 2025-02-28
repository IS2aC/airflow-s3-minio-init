"""workflow to push data on minio using python and s3 hook"""

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime


LOCAL_FILE_PATH = "/usr/local/airflow/include/send_on_minio.txt"
BUCKET_NAME = "miniobucket"
S3_KEY = "send_on_minio.txt"


def upload_file_on_minio():
    hook = S3Hook(aws_conn_id="minio_default")  # Connexion définie dans Airflow
    client = hook.get_conn()  # Récupération du client boto3

    if not hook.check_for_bucket(BUCKET_NAME):
        client.create_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' a été créé.")

    #S3_KEY doit être un nom de fichier valide
    S3_KEY = f"send_on_minio_{datetime.now()}.txt"

    #Utilisation correcte de load_file
    hook.load_file(filename=LOCAL_FILE_PATH, key=S3_KEY, bucket_name=BUCKET_NAME, replace=True)

    print(f"Fichier '{LOCAL_FILE_PATH}' uploadé en tant que '{S3_KEY}' dans '{BUCKET_NAME}'.")



# Définition du DAG
with DAG(
    dag_id="send_file_minio",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    send_text_file =  PythonOperator(
        task_id = "send_text_file",
        python_callable =  upload_file_on_minio
    )

    send_text_file