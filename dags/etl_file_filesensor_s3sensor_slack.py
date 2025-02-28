""" workflow etl using filesensor, s3transfers and slack webhook """

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta



PATH_FILE_CSV = '/usr/local/airflow/include/data/fake-data.csv'



default_args = {
    'owner': 'is2ac',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'etl',
    default_args = default_args,
    schedule_interval = None,
    catchup = False
) as dag:

    # Sensor file
    file_sensor_task = FileSensor(
        task_id = 'wait_on_data_repertory',
        filepath = PATH_FILE_CSV, 
        poke_interval=20, 
        timeout=600,  
        mode='reschedule',
        fs_conn_id = "fs_conn_defaut",
    )

    # local to s3
    push_data_to_minio = LocalFilesystemToS3Operator(
        task_id='push_data_to_minio',
        filename=PATH_FILE_CSV,  # Utilise 'filename' au lieu de 'local_file'
        dest_bucket='datacatalog',  # Utilise 'dest_bucket' au lieu de 's3_bucket'
        dest_key='fake-data.csv',  # Utilise 'dest_key' au lieu de 's3_key'
        aws_conn_id="minio_default"
    )


    # http request over slack webhook
    alert_slack = BashOperator(
        task_id='alert_slack',
        bash_command="""curl -X POST -H 'Content-type: application/json' --data '{"text":"Données détecter par notre sensor et inserer directement ..."}' https://hooks.slack.com/services/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""",
    )
    

file_sensor_task >> push_data_to_minio >> alert_slack