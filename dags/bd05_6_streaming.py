"""DAG 6 - Streaming: Spark Structured Streaming đọc Kafka 'news-topic' -> Bronze/Silver streaming.

Chạy theo lịch 15 phút/lần ở chế độ availableNow: xử lý hết message mới (checkpoint trên HDFS
bảo đảm không xử lý lặp) rồi dừng. Message được phát bằng Kafka Producer Replay
(src/streaming/kafka_producer_news.py hoặc nút trên Dashboard).
"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from lakehouse_common import DEFAULT_ARGS, spark_job, task_sync_gold

with DAG("bd05_6_streaming", default_args=DEFAULT_ARGS, schedule_interval=timedelta(minutes=15), catchup=False,
         max_active_runs=1, tags=["bd05", "kafka", "streaming"],
         description="Kafka -> Spark Structured Streaming -> HDFS") as dag:
    stream = PythonOperator(task_id="kafka_structured_streaming",
                            python_callable=spark_job("src/streaming/spark_streaming_job.py", {"STREAM_MODE": "once"}))
    sync = PythonOperator(task_id="sync_gold", python_callable=task_sync_gold)
    stream >> sync
