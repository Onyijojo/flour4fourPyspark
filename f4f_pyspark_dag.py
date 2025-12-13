from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import the functions from your project folder (mounted into /opt/airflow/dags/flour4flour)
from f4f.extract import run_extraction
from f4f.transform import run_transformation
from f4f.load import run_loading

default_args = {
    "owner": "onyeka",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="Flour4fout_pyspark",
    default_args=default_args,
    start_date=datetime(2025, 12, 10),
    schedule="@daily",
    catchup=False,
    tags=["etl", "F4f", "pyspark"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=run_extraction,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=run_transformation,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=run_loading,
    )

    extract_task >> transform_task >> load_task
