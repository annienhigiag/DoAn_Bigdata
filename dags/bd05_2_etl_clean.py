"""DAG 2 - ETL Clean: làm sạch, Unicode NFC, Exact + SimHash dedup (Bronze -> Silver)."""
from airflow import DAG
from airflow.operators.python import PythonOperator

from lakehouse_common import DEFAULT_ARGS, spark_job

with DAG("bd05_2_etl_clean", default_args=DEFAULT_ARGS, schedule_interval=None, catchup=False,
         max_active_runs=1, tags=["bd05", "etl", "spark"],
         description="Bronze -> Silver: làm sạch & khử trùng lặp") as dag:
    PythonOperator(task_id="bronze_to_silver", python_callable=spark_job("src/jobs/bronze_to_silver_job.py"))
