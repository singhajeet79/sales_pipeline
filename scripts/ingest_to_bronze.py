import boto3
import os

def ingest_local_to_s3():
    # Path inside the Airflow container
    LOCAL_FILE = "/opt/airflow/data/sales.csv"
    BUCKET_NAME = "bytevault-ade-bronze-ap-south-1"
    S3_KEY = "sales/sales.csv"

    s3 = boto3.client('s3')

    if os.path.exists(LOCAL_FILE):
        print(f"📂 Found: {LOCAL_FILE}. Uploading...")
        s3.upload_file(LOCAL_FILE, BUCKET_NAME, S3_KEY)
        print("✅ Ingestion Successful!")
    else:
        raise FileNotFoundError(f"❌ {LOCAL_FILE} not found!")

if __name__ == "__main__":
    ingest_local_to_s3()
