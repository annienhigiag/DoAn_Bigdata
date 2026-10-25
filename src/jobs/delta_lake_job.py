"""
Delta Lake trên HDFS: chuyển bảng Silver đã gán nhãn sang Delta và minh họa
ACID / MERGE / Time-travel / OPTIMIZE (Z-ORDER) / VACUUM.

  1. Ghi đè silver/news_sentiment.parquet -> silver/bai_viet_delta (Delta, phân vùng theo ngày)
  2. MERGE nhãn sửa tay từ Dashboard (labels/feedback.csv: id,label) vào bảng Delta
  3. Time-travel: so sánh phiên bản ngay sau khi nạp với phiên bản mới nhất
  4. OPTIMIZE ... ZORDER BY (source): gộp file nhỏ, sắp theo nguồn báo
  5. VACUUM: dọn file không còn được tham chiếu (giữ 168 giờ để còn time-travel)
  6. Lưu lịch sử phiên bản vào gold/delta_lich_su.parquet cho Dashboard
"""
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.models.sentiment_features import LABEL_NAMES
from src.utils import lakehouse_paths as P
from src.utils.spark_helper import path_exists


def create_spark_session():
    return SparkSession.builder \
        .appName("Lakehouse_Delta_Lake") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .getOrCreate()


def label_distribution(df):
    return {r["sentiment_label"]: r["count"] for r in df.groupBy("sentiment_label").count().collect()}


def main():
    from delta.tables import DeltaTable

    start = time.time()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    path = P.SILVER_DELTA
    print("=" * 70)
    print(f" DELTA LAKE: {path}")
    print("=" * 70)

    # 1. Nạp Silver vào Delta
    print("\n[1/6] Ghi Silver (Parquet) thành bảng Delta phân vùng theo ngày...")
    silver = spark.read.parquet(P.SILVER_SENTIMENT).drop("content")
    silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .partitionBy("crawled_date").save(path)
    table = DeltaTable.forPath(spark, path)
    load_version = table.history(1).collect()[0]["version"]
    print(f"-> Phiên bản sau khi nạp: v{load_version} ({spark.read.format('delta').load(path).count():,} bài)")

    # 2. MERGE nhãn sửa tay (write-back)
    print("\n[2/6] MERGE nhãn sửa tay từ Dashboard (write-back)...")
    if path_exists(spark, P.LABELS_FEEDBACK):
        fb = spark.read.option("header", True).csv(P.LABELS_FEEDBACK) \
            .select(F.col("id").cast("long").alias("id"), F.col("label").cast("int").alias("label"), "updated_at") \
            .filter(F.col("label").isin(0, 1, 2))
        # Mỗi bài chỉ giữ lần sửa mới nhất
        latest_first = Window.partitionBy("id").orderBy(F.desc("updated_at"))
        fb = fb.withColumn("_rn", F.row_number().over(latest_first)).filter("_rn = 1").drop("_rn")
        label_name = F.create_map(*[x for k, v in LABEL_NAMES.items() for x in (F.lit(k), F.lit(v))])
        fb = fb.withColumn("label_name", label_name[F.col("label")])
        n_fb = fb.count()
        table.alias("t").merge(fb.alias("s"), "t.id = s.id").whenMatchedUpdate(set={
            "sentiment_class": "s.label",
            "sentiment_label": "s.label_name",
            "sentiment_score": F.lit(1.0),
        }).execute()
        print(f"-> Đã MERGE {n_fb} nhãn sửa tay")
    else:
        print(f"-> Chưa có {P.LABELS_FEEDBACK}, bỏ qua MERGE")

    # 3. Time-travel
    print("\n[3/6] Time-travel: so sánh phiên bản lúc nạp và phiên bản mới nhất...")
    latest_version = table.history(1).collect()[0]["version"]
    v_old = spark.read.format("delta").option("versionAsOf", load_version).load(path)
    v_new = spark.read.format("delta").load(path)
    print(f"   v{load_version}: {label_distribution(v_old)}")
    print(f"   v{latest_version}: {label_distribution(v_new)}")

    # 4. OPTIMIZE + Z-ORDER
    print("\n[4/6] OPTIMIZE ... ZORDER BY (source)...")
    metrics = table.optimize().executeZOrderBy("source").select("metrics.numFilesAdded", "metrics.numFilesRemoved").collect()[0]
    print(f"-> File thêm: {metrics['numFilesAdded']}, file gộp/bỏ: {metrics['numFilesRemoved']}")

    # 5. VACUUM
    print("\n[5/6] VACUUM...")
    would_delete = spark.sql(f"VACUUM delta.`{path}` RETAIN 0 HOURS DRY RUN").count()
    print(f"-> Nếu VACUUM RETAIN 0 HOURS sẽ xóa {would_delete} file cũ (chỉ chạy thử, để còn time-travel)")
    table.vacuum(168)
    print("-> Đã VACUUM với thời hạn giữ 168 giờ")

    # 6. Lịch sử phiên bản
    print("\n[6/6] Lịch sử phiên bản Delta:")
    history = table.history().select(
        "version", F.col("timestamp").cast("string").alias("timestamp"), "operation",
        F.to_json("operationParameters").alias("operation_parameters"),
        F.to_json("operationMetrics").alias("operation_metrics"),
    ).orderBy("version")
    history.select("version", "timestamp", "operation").show(30, truncate=False)
    history.coalesce(1).write.mode("overwrite").parquet(P.GOLD_DELTA_HISTORY)

    print(f" HOÀN TẤT DELTA LAKE TRONG {time.time() - start:.1f} GIÂY")
    spark.stop()


if __name__ == "__main__":
    main()
