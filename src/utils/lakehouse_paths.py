"""
Đường dẫn chuẩn của Lakehouse trên HDFS, dùng chung cho mọi Spark job.
Có thể đổi gốc HDFS bằng biến môi trường LAKEHOUSE_ROOT (ví dụ khi chạy thử local).
"""
import os

LAKEHOUSE_ROOT = os.environ.get("LAKEHOUSE_ROOT", "hdfs://namenode:9000/lakehouse/bd05_tin_tuc")

BRONZE_NEWS = f"{LAKEHOUSE_ROOT}/bronze/news_2021_2022.parquet"

SILVER_CLEAN = f"{LAKEHOUSE_ROOT}/silver/bai_viet_clean.parquet"
SILVER_SENTIMENT = f"{LAKEHOUSE_ROOT}/silver/news_sentiment.parquet"

GOLD_TREND = f"{LAKEHOUSE_ROOT}/gold/xu_huong_ngay.parquet"
GOLD_ALERT = f"{LAKEHOUSE_ROOT}/gold/canh_bao_dot_bien.parquet"
GOLD_ETL_STATS = f"{LAKEHOUSE_ROOT}/gold/etl_quality_stats.parquet"
GOLD_MODEL_METRICS = f"{LAKEHOUSE_ROOT}/gold/sentiment_model_metrics.parquet"

QUARANTINE = f"{LAKEHOUSE_ROOT}/_quarantine/bronze_to_silver.parquet"

MODEL_SENTIMENT = f"{LAKEHOUSE_ROOT}/models/sentiment_model"

# Nhãn có sẵn (CSV, cột: text,label với 0=Tiêu cực, 1=Trung tính, 2=Tích cực)
LABELS_TRAIN = f"{LAKEHOUSE_ROOT}/labels/sentiment_train.csv"
# 200 bài báo gán nhãn tay (CSV, cột: id,label) để đo domain shift
LABELS_MANUAL = f"{LAKEHOUSE_ROOT}/labels/manual_200.csv"
LABELS_MANUAL_SAMPLE = f"{LAKEHOUSE_ROOT}/labels/manual_200_sample.csv"

# Phân tích nâng cao
GOLD_LDA_TOPICS = f"{LAKEHOUSE_ROOT}/gold/lda_chu_de.parquet"
GOLD_LDA_DAILY = f"{LAKEHOUSE_ROOT}/gold/lda_chu_de_theo_ngay.parquet"
GOLD_ENTITY_ALERT = f"{LAKEHOUSE_ROOT}/gold/canh_bao_thuc_the.parquet"
GOLD_ENTITY_TOP = f"{LAKEHOUSE_ROOT}/gold/thuc_the_noi_bat.parquet"
GOLD_ARTICLES = f"{LAKEHOUSE_ROOT}/gold/bai_viet_tieu_bieu.parquet"
GOLD_DELTA_HISTORY = f"{LAKEHOUSE_ROOT}/gold/delta_lich_su.parquet"
GOLD_STREAM_STATS = f"{LAKEHOUSE_ROOT}/gold/streaming_thong_ke.parquet"
MODEL_LDA = f"{LAKEHOUSE_ROOT}/models/lda_model"

# Delta Lake
SILVER_DELTA = f"{LAKEHOUSE_ROOT}/silver/bai_viet_delta"

# Write-back từ Dashboard (CSV cột: id,label,note,updated_at)
LABELS_FEEDBACK = f"{LAKEHOUSE_ROOT}/labels/feedback.csv"

# Streaming từ Kafka
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "news-topic")
BRONZE_STREAM = f"{LAKEHOUSE_ROOT}/bronze/streaming/news"
SILVER_STREAM = f"{LAKEHOUSE_ROOT}/silver/streaming_sentiment"
CHECKPOINT_BRONZE_STREAM = f"{LAKEHOUSE_ROOT}/checkpoints/bronze_stream"
CHECKPOINT_SILVER_STREAM = f"{LAKEHOUSE_ROOT}/checkpoints/silver_stream"
