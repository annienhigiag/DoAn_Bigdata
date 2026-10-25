"""DAG 4 - Scoring & Gold: gán nhãn cảm xúc Silver, Spark SQL, bảng Gold xu hướng + Z-score."""
from airflow import DAG
from airflow.operators.python import PythonOperator

from lakehouse_common import DEFAULT_ARGS, spark_job, task_sync_gold

with DAG("bd05_4_scoring_gold", default_args=DEFAULT_ARGS, schedule_interval=None, catchup=False,
         max_active_runs=1, tags=["bd05", "scoring", "gold"],
         description="Silver scoring -> Spark SQL -> Gold xu hướng & cảnh báo đột biến") as dag:
    scoring = PythonOperator(task_id="sentiment_scoring",
                             python_callable=spark_job("src/jobs/silver_sentiment_inference.py"))
    sql = PythonOperator(task_id="spark_sql_analytics", python_callable=spark_job("src/jobs/spark_sql_analytics.py"))
    gold = PythonOperator(task_id="publish_gold", python_callable=spark_job("src/jobs/silver_to_gold_job.py"))
    sync = PythonOperator(task_id="sync_gold", python_callable=task_sync_gold)
    scoring >> sql >> gold >> sync
