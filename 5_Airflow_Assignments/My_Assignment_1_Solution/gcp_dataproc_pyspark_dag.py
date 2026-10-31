from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 10, 24),
}

dag = DAG(
    "Assignment_1_DAG",
    default_args=default_args,
    description="Main dag to process csv files using pyspark",
    schedule_interval = "0 0 * * *", # Job runs everyday at 12:00 AM
    catchup=False,
    tags=['assignment']
)
 
# Define cluster config
CLUSTER_NAME = 'assignment-1-cluster'
PROJECT_ID = 'project-86f9f620-7340-4e1c-921'
REGION = 'us-central1'
CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1, # Master node
        'machine_type_uri': 'n4d-standard-2',  # Machine type
        'disk_config': {
            'boot_disk_type': 'hyperdisk-balanced',
            'boot_disk_size_gb': 30
        }
    },
    'worker_config': {
        'num_instances': 2,  # Worker nodes
        'machine_type_uri': 'n4d-standard-2',  # Machine type
        'disk_config': {
            'boot_disk_type': 'hyperdisk-balanced',
            'boot_disk_size_gb': 30
        }
    },
    'software_config': {
        'image_version': '2.3.37-debian12'  # Image version
    }
}

