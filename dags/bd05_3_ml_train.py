"""DAG 3 - Huấn luyện & đánh giá mô hình cảm xúc Spark MLlib (UIT-VSFC + nhãn sửa tay)."""
from airflow import DAG
from airflow.operators.python import PythonOperator

from lakehouse_common import DEFAULT_ARGS, spark_job, task_upload_feedback

with DAG("bd05_3_ml_train", default_args=DEFAULT_ARGS, schedule_interval=None, catchup=False,
         max_active_runs=1, tags=["bd05", "mllib", "train"],
         description="Huấn luyện TF-IDF + LogisticRegression, đo Macro-F1, lưu model lên HDFS") as dag:
    feedback = PythonOperator(task_id="upload_feedback_labels", python_callable=task_upload_feedback)
    train = PythonOperator(task_id="train_sentiment_model",
                           python_callable=spark_job("src/models/train_sentiment_mllib.py"))
    feedback >> train
