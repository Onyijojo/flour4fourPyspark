from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import the functions from your project folder (mounted into /opt/airflow/dags/f4f)
from Flour4fourPyspark.extraction import run_extraction
from Flour4fourPyspark.transformation import run_transformation
from Flour4fourPyspark.loading import run_loading

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="F4f_pyspark_dag",
    default_args=default_args,
    start_date=datetime(2025, 12, 10),
    schedule=None,
    catchup=False,
    tags=["etl", "Pyspark"],
) as dag:

    extraction = PythonOperator(
        task_id='extraction',
        python_callable=run_extraction,
    )

    transformation = PythonOperator(
        task_id='transformation',
        python_callable=run_transformation,
    )

    loading = PythonOperator(
        task_id='loading',
        python_callable=run_loading,
    )

    extraction >> transformation >> loading
