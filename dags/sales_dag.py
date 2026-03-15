from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# AWS Configurations
AWS_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") 
S3_ENDPOINT = "s3.ap-south-1.amazonaws.com"

default_args = {
    "owner": "data-eng",
    "retries": 1
}

SPARK_SUBMIT_BASE = (
    "docker exec de-master-lab /spark/bin/spark-submit "
    "--conf spark.driver.extraClassPath=/spark/jars/* "
    "--conf spark.executor.extraClassPath=/spark/jars/* "
    f"--conf spark.hadoop.fs.s3a.endpoint={S3_ENDPOINT} "
    f"--conf spark.hadoop.fs.s3a.access.key={AWS_ID} "
    f"--conf spark.hadoop.fs.s3a.secret.key={AWS_KEY} "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.hadoop.fs.s3a.path.style.access=false "
)

with DAG(
    dag_id="sales_medallion_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args
) as dag:

    # 1. Wait for local sales.csv
    wait_for_file = FileSensor(
        task_id="wait_for_sales_csv",
        filepath="/opt/airflow/data/sales.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        mode='poke'
    )

    # 2. Ingest local file to S3 Bronze
    ingest_to_s3 = BashOperator(
        task_id="ingest_to_s3",
        bash_command="python3 /opt/airflow/scripts/sales/ingest_to_bronze.py"
    )

    # 3. Transform Bronze -> Silver
    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=f"{SPARK_SUBMIT_BASE} /home/jovyan/projects/sales_pipeline/scripts/transform_silver.py"
    )

    # 4. Aggregate Silver -> Gold
    aggregate_gold = BashOperator(
        task_id="aggregate_gold",
        bash_command=f"{SPARK_SUBMIT_BASE} /home/jovyan/projects/sales_pipeline/scripts/aggregate_gold.py"
    )

    wait_for_file >> ingest_to_s3 >> transform_silver >> aggregate_gold
