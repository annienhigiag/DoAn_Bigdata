"""DAG 1 - Batch ingestion: nạp Bronze (replication 3) và các tập nhãn lên HDFS."""
from airflow import DAG
from airflow.operators.python import PythonOperator

from lakehouse_common import DEFAULT_ARGS, task_ingest_bronze, task_upload_labels

with DAG("bd05_1_ingestion", default_args=DEFAULT_ARGS, schedule_interval=None, catchup=False,
         max_active_runs=1, tags=["bd05", "ingestion", "hdfs"],
         description="Nạp dữ liệu thô vào HDFS Bronze và nhãn vào labels/") as dag:
    ingest = PythonOperator(task_id="ingest_bronze", python_callable=task_ingest_bronze)
    labels = PythonOperator(task_id="upload_labels", python_callable=task_upload_labels)
    ingest >> labels
