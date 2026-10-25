"""
DAG ĐIỀU PHỐI TỔNG: chạy lần lượt 5 DAG batch, mỗi bước đợi DAG trước hoàn tất.

  bd05_1_ingestion -> bd05_2_etl_clean -> bd05_3_ml_train -> bd05_4_scoring_gold
                   -> bd05_5_advanced_analytics

DAG bd05_6_streaming chạy độc lập theo lịch 15 phút. Tất cả DAG đều có thể trigger từ
Dashboard (tab "Pipeline & Write-back") thông qua FastAPI -> Airflow REST API.
"""
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from lakehouse_common import DEFAULT_ARGS

STEPS = ["bd05_1_ingestion", "bd05_2_etl_clean", "bd05_3_ml_train", "bd05_4_scoring_gold",
         "bd05_5_advanced_analytics"]

with DAG("lakehouse_master_pipeline", default_args=DEFAULT_ARGS, schedule_interval="@daily", catchup=False,
         max_active_runs=1, tags=["bd05", "master"],
         description="Điều phối toàn bộ pipeline batch Lakehouse") as dag:
    prev = EmptyOperator(task_id="start")
    for dag_id in STEPS:
        step = TriggerDagRunOperator(
            task_id=f"run_{dag_id}",
            trigger_dag_id=dag_id,
            wait_for_completion=True,
            poke_interval=20,
            reset_dag_run=True,
            allowed_states=["success"],
            failed_states=["failed"],
            # Hoãn (defer) trong lúc chờ để nhả slot của executor cho DAG con chạy
            # (Airflow standalone dùng SequentialExecutor: mỗi lúc chỉ chạy 1 task)
            deferrable=True,
        )
        prev >> step
        prev = step
    prev >> EmptyOperator(task_id="end")
