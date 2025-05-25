# EU Tourism ETL Pipeline

An end-to-end data pipeline built with **Apache Airflow**, **Papermill**, and **PySpark** that:

1. Ingests raw CSV tourism data from an S3 bucket  
2. Cleans, transforms and writes staging tables to Postgres  
3. Runs aggregations, modeling, clustering and map visualizations  
4. Is orchestrated daily via an Airflow DAG  

---

## 📦 What’s Inside

- **`dags/`** – your Airflow DAG (`tourism_simple_pipeline.py`)  
- **`notebooks/`** – Papermill-parameterized Jupyter notebooks (no outputs)  
- **`docker-compose.yaml`** – spins up Airflow & Postgres locally  
- **`requirements.txt`** – Python dependencies  
- **`.env.example`** – template for your credentials & connection settings  
- **`.gitignore`** – excludes secrets, notebook outputs, venvs, DB files

---

## 🚀 Quickstart

1. **Clone** this repo  
   ```bash
   git clone https://github.com/NarendraSinghChilwal/EU_tourism_ETL.git
   cd EU_tourism_ETL

cat << 'EOF' > README.md
# EU Tourism ETL Pipeline

An end-to-end data pipeline built with **Apache Airflow**, **Papermill**, and **PySpark** that:

1. Ingests raw CSV tourism data from an S3 bucket  
2. Cleans, transforms and writes staging tables to Postgres  
3. Runs aggregations, modeling, clustering and map visualizations  
4. Is orchestrated daily via an Airflow DAG  

---

## 📦 What’s Inside

- **`dags/`** – your Airflow DAG (`tourism_simple_pipeline.py`)  
- **`notebooks/`** – Papermill-parameterized Jupyter notebooks (no outputs)  
- **`docker-compose.yaml`** – spins up Airflow & Postgres locally  
- **`requirements.txt`** – Python dependencies  
- **`.env.example`** – template for your credentials & connection settings  
- **`.gitignore`** – excludes secrets, notebook outputs, venvs, DB files  

---

## 🚀 Quickstart

1. **Clone** this repo  
   ```bash
   git clone https://github.com/NarendraSinghChilwal/EU_tourism_ETL.git
   cd EU_tourism_ETL

cp .env.example .env
# then edit `.env` and replace placeholders with your AWS & Postgres creds

docker compose up --build

