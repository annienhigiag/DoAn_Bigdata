"""
Spark Structured Streaming: Kafka topic 'news-topic' -> Bronze (thô) + Silver (đã làm sạch, gán nhãn cảm xúc).

  Kafka ──readStream──> parse JSON ──┬──> bronze/streaming/news          (Parquet, append, checkpoint)
                                     └──> làm sạch + MLlib sentiment ──> silver/streaming_sentiment
                                                                         (Parquet theo ngày, append, checkpoint)
Mỗi micro-batch còn ghi thống kê (số bài, phân bố cảm xúc) vào gold/streaming_thong_ke.parquet.

Chế độ chạy:
  STREAM_MODE=once (mặc định): trigger(availableNow=True) xử lý hết dữ liệu đang có rồi dừng
                               (dùng trong DAG Airflow).
  STREAM_MODE=continuous     : chạy liên tục, micro-batch mỗi 10 giây (dùng khi demo).
Checkpoint trên HDFS bảo đảm mỗi message chỉ được xử lý đúng một lần giữa các lần chạy.
"""
import os
import sys

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.models.sentiment_features import LABEL_NAMES, build_article_text, prepare_text
from src.nlp.nlp_cleaner import clean_text_spark
from src.utils import lakehouse_paths as P
from src.utils.spark_helper import path_exists

EVENT_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("title", StringType()),
    StructField("content", StringType()),
    StructField("source", StringType()),
    StructField("topic", StringType()),
    StructField("url", StringType()),
    StructField("crawled_at", StringType()),
])


def create_spark_session():
    return SparkSession.builder \
        .appName("Lakehouse_Kafka_Structured_Streaming") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    mode = os.environ.get("STREAM_MODE", "once")

    print("=" * 70)
    print(f" SPARK STRUCTURED STREAMING: Kafka {P.KAFKA_BOOTSTRAP} / topic '{P.KAFKA_TOPIC}' (mode={mode})")
    print("=" * 70)

    raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", P.KAFKA_BOOTSTRAP) \
        .option("subscribe", P.KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    events = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
    ).select("kafka_key", "kafka_partition", "kafka_offset", "kafka_ts", "e.*") \
        .withColumn("crawled_at", F.coalesce(F.to_timestamp("crawled_at"), F.col("kafka_ts"))) \
        .withColumn("ingest_date", F.to_date("kafka_ts"))

    # Nhánh 1: Bronze thô
    trigger = {"availableNow": True} if mode == "once" else {"processingTime": "10 seconds"}
    bronze_q = events.writeStream.format("parquet") \
        .option("path", P.BRONZE_STREAM) \
        .option("checkpointLocation", P.CHECKPOINT_BRONZE_STREAM) \
        .partitionBy("ingest_date") \
        .outputMode("append") \
        .trigger(**trigger) \
        .queryName("bronze_stream") \
        .start()

    # Nhánh 2: làm sạch + gán nhãn cảm xúc bằng mô hình đã huấn luyện
    model = PipelineModel.load(P.MODEL_SENTIMENT)
    cleaned = clean_text_spark(events, "title", "title")
    cleaned = clean_text_spark(cleaned, "content", "content") \
        .withColumn("source", F.lower(F.trim("source"))) \
        .withColumn("crawled_date", F.to_date("crawled_at")) \
        .filter(F.length("title") > 5)
    label_name = F.create_map(*[x for k, v in LABEL_NAMES.items() for x in (F.lit(float(k)), F.lit(v))])
    scored = model.transform(prepare_text(build_article_text(cleaned))) \
        .withColumn("sentiment_class", F.col("prediction").cast("int")) \
        .withColumn("sentiment_label", label_name[F.col("prediction")]) \
        .withColumn("sentiment_score", F.round(F.array_max(vector_to_array("probability")), 4)) \
        .select("id", "url", "source", "topic", "title", "sentiment_class", "sentiment_label",
                "sentiment_score", "crawled_at", "crawled_date", "kafka_offset", "kafka_ts")

    def write_silver_batch(batch_df, batch_id):
        batch_df = batch_df.cache()
        n = batch_df.count()
        if n == 0:
            batch_df.unpersist()
            return
        batch_df.write.mode("append").partitionBy("crawled_date").parquet(P.SILVER_STREAM)
        stats = batch_df.groupBy("sentiment_label").count().collect()
        dist = {r["sentiment_label"]: r["count"] for r in stats}
        spark.range(1).select(
            F.lit(batch_id).alias("batch_id"),
            F.current_timestamp().alias("processed_at"),
            F.lit(n).alias("so_bai"),
            F.lit(dist.get("Tích cực", 0)).alias("tich_cuc"),
            F.lit(dist.get("Trung tính", 0)).alias("trung_tinh"),
            F.lit(dist.get("Tiêu cực", 0)).alias("tieu_cuc"),
        ).write.mode("append").parquet(P.GOLD_STREAM_STATS)
        print(f"   [batch {batch_id}] {n} bài -> Silver streaming | {dist}")
        batch_df.unpersist()

    silver_q = scored.writeStream \
        .foreachBatch(write_silver_batch) \
        .option("checkpointLocation", P.CHECKPOINT_SILVER_STREAM) \
        .trigger(**trigger) \
        .queryName("silver_stream") \
        .start()

    if mode == "once":
        bronze_q.awaitTermination()
        silver_q.awaitTermination()
        for q in (bronze_q, silver_q):
            p = q.lastProgress
            print(f"-> {q.name}: {p['numInputRows'] if p else 0} message ở batch cuối")
    else:
        spark.streams.awaitAnyTermination()

    total = spark.read.parquet(P.SILVER_STREAM).count() if path_exists(spark, P.SILVER_STREAM) else 0
    print(f" HOÀN TẤT. Tổng số bài trong {P.SILVER_STREAM}: {total:,}")
    spark.stop()


if __name__ == "__main__":
    main()
