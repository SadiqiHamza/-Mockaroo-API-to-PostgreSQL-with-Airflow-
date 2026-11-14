from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import requests
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

# Retrieve API key from environment variable (set via Docker Compose)
API_KEY = os.getenv("MOCKAROO_API_KEY")

def extract():
    logger.info("Starting data extraction from Mockaroo.")

    if not API_KEY:
        logger.error("MOCKAROO_API_KEY is not set.")
        raise ValueError("MOCKAROO_API_KEY is not set.")

    url = "https://my.api.mockaroo.com/customers.json"
    headers = {"X-API-Key": API_KEY}

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status() 

        df = pd.DataFrame(response.json())
        records = df.to_dict(orient="records")

        return records

    except requests.RequestException:
        logger.exception("Data extraction from Mockaroo failed.")
        raise


# load data into postgres database
def load(records):
    logger.info("Starting database load of %d records.", len(records))

    df = pd.DataFrame(records)

    host="postgres"
    database= os.getenv("POSTGRES_DB")
    user= os.getenv("POSTGRES_USER")
    password= os.getenv("POSTGRES_PASSWORD")

    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:5432/{database}')
        df.to_sql('customers', engine, if_exists='append', index=False)
        logger.info("Successfully inserted %d records into PostgreSQL.", len(df))
    except Exception:
        logger.exception("Loading data into PostgreSQL failed.")
        raise


with DAG(
    dag_id="api_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/1 * * * *",
    catchup=False,
    default_args={"retries": 1},
) as dag:
    extract_operator = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    load_operator = PythonOperator(
        task_id="load",
        python_callable=load,
        op_kwargs={"records": extract_operator.output},
    )

    extract_operator >> load_operator