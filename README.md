# ByteVault: Sales Medallion Data Pipeline 🚀

An end-to-end Data Engineering platform leveraging the **Medallion Architecture** (Bronze → Silver → Gold) to process retail sales data. This project uses a containerized infrastructure to orchestrate data movement from local sources to AWS S3 and PostgreSQL.



## 🏗️ Architecture Stack
* **Orchestration:** Apache Airflow 2.9.1 (LocalExecutor)
* **Processing Engine:** Apache Spark (PySpark)
* **Storage:** AWS S3 (Data Lake) & PostgreSQL (Data Warehouse)
* **Environment:** Docker & Docker Compose
* **Development:** Jupyter Lab (DE Master Lab)

## 🔄 Pipeline Workflow
1.  **File Sensing:** Airflow `FileSensor` monitors a local directory for `sales.csv`.
2.  **Ingestion (Bronze):** A Python/Boto3 script uploads raw data to the **AWS S3 Bronze Bucket**.
3.  **Transformation (Silver):** PySpark cleans the data, handles nulls, and casts types, saving it as Parquet in the **Silver Bucket**.
4.  **Aggregation (Gold):** PySpark calculates business metrics (Total Revenue by Product) and sinks the final results into **Postgres** and the **Gold Bucket**.

---

## 🛠️ Setup & Installation

### 1. Prerequisites
* Docker & Docker Compose installed.
* AWS IAM User with S3 Full Access.

### 2. Environment Configuration
Create a `.env` file in the root directory:
```env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=ap-south-1

---

### 3. Launching the Platform
```
docker compose -f master-platform.yml up -d

*Airflow UI: localhost:8080 (admin/admin)

* Jupyter Lab: localhost:8888

---

### 📂 Project Structure
```
shared-infra/
├── master-platform.yml        # Docker Compose infrastructure
├── sales_pipeline/
│   ├── dags/                  # Airflow DAG definitions
│   ├── scripts/               # PySpark & Ingestion scripts
│   └── data/                  # Landing zone for source CSVs

---

### 🚀 Future Roadmap
[ ] Implement Great Expectations for data quality checks.

[ ] Transition from LocalExecutor to CeleryExecutor for scaling.

[ ] Add a dbt layer for Gold transformations.


---

#### Check out [My GitHub Page](https://singhajeet79.github.io/) for more AI/MLOps/Data Engineering projects.
**Happy Engineering!** 🚀
