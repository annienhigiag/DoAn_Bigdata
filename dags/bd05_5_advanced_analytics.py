"""DAG 5 - Phân tích nâng cao: Delta Lake (MERGE write-back, time-travel, OPTIMIZE, VACUUM),
đột biến theo thực thể/thương hiệu, chủ đề LDA."""
from airflow import DAG
from airflow.operators.python import PythonOperator

from lakehouse_common import DEFAULT_ARGS, spark_job, task_sync_gold, task_upload_feedback

with DAG("bd05_5_advanced_analytics", default_args=DEFAULT_ARGS, schedule_interval=None, catchup=False,
         max_active_runs=1, tags=["bd05", "delta", "lda", "entity"],
         description="Delta Lake + đột biến thực thể + LDA topic") as dag:
    feedback = PythonOperator(task_id="upload_feedback_labels", python_callable=task_upload_feedback)
    delta = PythonOperator(task_id="delta_lake", python_callable=spark_job("src/jobs/delta_lake_job.py"))
    entity = PythonOperator(task_id="entity_trends", python_callable=spark_job("src/jobs/entity_trend_job.py"))
    lda = PythonOperator(task_id="lda_topics", python_callable=spark_job("src/jobs/lda_topic_job.py"))
    sync = PythonOperator(task_id="sync_gold", python_callable=task_sync_gold)
    feedback >> delta >> entity >> lda >> sync
