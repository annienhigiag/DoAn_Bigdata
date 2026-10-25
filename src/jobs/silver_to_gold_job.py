import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.utils import lakehouse_paths as P


def create_spark_session():
    """
    Khởi tạo SparkSession tối ưu hóa tổng hợp số liệu tầng Gold.
    """
    return SparkSession.builder \
        .appName("Lakehouse_Gold_Aggregation_and_Anomaly_Detection") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def main():
    start_time = time.time()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_sentiment_path = os.environ.get("SILVER_SENTIMENT_PATH", P.SILVER_SENTIMENT)
    gold_trend_path = os.environ.get("GOLD_TREND_PATH", P.GOLD_TREND)
    gold_anomaly_path = os.environ.get("GOLD_ALERT_PATH", P.GOLD_ALERT)

    print("=" * 75)
    print(" BẮT ĐẦU PIPELINE TỔNG HỢP TẦNG GOLD & PHÁT HIỆN ĐỘT BIẾN Z-SCORE")
    print(f" Nguồn vào (Silver) : {silver_sentiment_path}")
    print(f" Đích Bảng 1 (Trend): {gold_trend_path}")
    print(f" Đích Bảng 2 (Alert): {gold_anomaly_path}")
    print("=" * 75)

    # 1. ĐỌC DỮ LIỆU TẦNG SILVER ĐÃ GÁN NHÃN CẢM XÚC
    print("\n[Bước 1/5] Đang nạp dữ liệu gán nhãn tầng Silver từ HDFS...")
    silver_df = spark.read.parquet(silver_sentiment_path)
    total_articles = silver_df.count()
    print(f"-> Tổng số bài báo nạp vào: {total_articles:,} bài")

    # 2. XÂY DỰNG BẢNG 1: gold.xu_huong_ngay
    # Tổng hợp theo: crawled_date, topic, source
    print("\n[Bước 2/5] Đang tổng hợp Bảng 1: gold.xu_huong_ngay (Đa chiều: Ngày - Chủ đề - Nguồn báo)...")
    
    trend_df = silver_df.groupBy("crawled_date", "topic", "source") \
        .agg(
            F.count("*").alias("tong_so_bai"),
            F.sum(F.when(F.col("sentiment_label") == "Tích cực", 1).otherwise(0)).alias("so_bai_tich_cuc"),
            F.sum(F.when(F.col("sentiment_label") == "Trung tính", 1).otherwise(0)).alias("so_bai_trung_tinh"),
            F.sum(F.when(F.col("sentiment_label") == "Tiêu cực", 1).otherwise(0)).alias("so_bai_tieu_cuc"),
            F.round(F.avg("sentiment_score"), 4).alias("diem_tin_cay_tb")
        ) \
        .withColumn("ty_le_tich_cuc", F.round(F.col("so_bai_tich_cuc") / F.col("tong_so_bai") * 100, 2)) \
        .withColumn("ty_le_trung_tinh", F.round(F.col("so_bai_trung_tinh") / F.col("tong_so_bai") * 100, 2)) \
        .withColumn("ty_le_tieu_cuc", F.round(F.col("so_bai_tieu_cuc") / F.col("tong_so_bai") * 100, 2))

    trend_record_count = trend_df.count()
    print(f"-> Đã tổng hợp thành công {trend_record_count:,} bản ghi xu hướng.")

    # Ghi Bảng 1 vào HDFS phân vùng theo ngày
    print(f"-> Đang ghi Bảng 1 ra HDFS: {gold_trend_path}...")
    trend_df.write \
        .mode("overwrite") \
        .partitionBy("crawled_date") \
        .parquet(gold_trend_path)
    print("-> Ghi Bảng 1 thành công!")

    # 3. XÂY DỰNG BẢNG 2: gold.canh_bao_dot_bien (Z-Score Anomaly Detection)
    # Tính Z-score tần suất bài viết và Z-score bài viết tiêu cực theo từng chủ đề (topic)
    print("\n[Bước 3/5] Đang tính toán chỉ số thống kê & Z-Score phát hiện đột biến truyền thông...")

    # Gom nhóm theo ngày và chủ đề
    daily_topic_df = silver_df.groupBy("crawled_date", "topic") \
        .agg(
            F.count("*").alias("so_bai_ngay"),
            F.sum(F.when(F.col("sentiment_label") == "Tiêu cực", 1).otherwise(0)).alias("so_bai_tieu_cuc"),
            F.sum(F.when(F.col("sentiment_label") == "Tích cực", 1).otherwise(0)).alias("so_bai_tich_cuc")
        )

    # Window phân vùng theo từng chuyên mục (topic) trên toàn bộ chuỗi 24 ngày
    topic_window = Window.partitionBy("topic")

    stats_df = daily_topic_df \
        .withColumn("avg_so_bai", F.avg("so_bai_ngay").over(topic_window)) \
        .withColumn("stddev_so_bai", F.coalesce(F.stddev("so_bai_ngay").over(topic_window), F.lit(1.0))) \
        .withColumn("avg_tieu_cuc", F.avg("so_bai_tieu_cuc").over(topic_window)) \
        .withColumn("stddev_tieu_cuc", F.coalesce(F.stddev("so_bai_tieu_cuc").over(topic_window), F.lit(1.0)))

    # Tính Z-score: Z = (x - mean) / stddev (Nếu stddev = 0 thì Z = 0)
    anomaly_df = stats_df \
        .withColumn(
            "z_score_volume",
            F.when(F.col("stddev_so_bai") > 0,
                   F.round((F.col("so_bai_ngay") - F.col("avg_so_bai")) / F.col("stddev_so_bai"), 2))
             .otherwise(0.0)
        ) \
        .withColumn(
            "z_score_crisis",
            F.when(F.col("stddev_tieu_cuc") > 0,
                   F.round((F.col("so_bai_tieu_cuc") - F.col("avg_tieu_cuc")) / F.col("stddev_tieu_cuc"), 2))
             .otherwise(0.0)
        ) \
        .withColumn(
            "muc_do_canh_bao",
            F.when((F.col("z_score_crisis") >= 2.0) | (F.col("z_score_volume") >= 2.0), "Khủng hoảng / Bùng nổ")
             .when((F.col("z_score_crisis") >= 1.5) | (F.col("z_score_volume") >= 1.5), "Cảnh báo / Tăng cao")
             .otherwise("Bình thường")
        ) \
        .select(
            F.col("crawled_date"),
            F.col("topic"),
            F.col("so_bai_ngay"),
            F.col("so_bai_tieu_cuc"),
            F.col("so_bai_tich_cuc"),
            F.round(F.col("avg_so_bai"), 2).alias("tb_so_bai_chuyen_muc"),
            F.round(F.col("stddev_so_bai"), 2).alias("do_lech_chuan_so_bai"),
            F.col("z_score_volume"),
            F.col("z_score_crisis"),
            F.col("muc_do_canh_bao")
        )

    # 4. GHI BẢNG 2 VÀO HDFS PHÂN VÙNG THEO NGÀY
    print(f"\n[Bước 4/5] Đang ghi Bảng 2 ra HDFS: {gold_anomaly_path}...")
    anomaly_df.write \
        .mode("overwrite") \
        .partitionBy("crawled_date") \
        .parquet(gold_anomaly_path)
    print("-> Ghi Bảng 2 thành công!")

    # Bảng 3: bài viết tiêu biểu cho Dashboard (xem chi tiết đột biến, sửa nhãn write-back).
    # Gồm các bài thuộc (ngày, chủ đề) đang cảnh báo + mẫu ngẫu nhiên, tối đa 5.000 bài.
    hot = anomaly_df.filter(F.col("muc_do_canh_bao") != "Bình thường").select("crawled_date", "topic")
    article_cols = ["id", "url", "source", "topic", "title", F.substring("content", 1, 300).alias("sapo"),
                    "sentiment_class", "sentiment_label", "sentiment_score", "crawled_date"]
    in_alert = silver_df.join(F.broadcast(hot), ["crawled_date", "topic"]).select(*article_cols)         .withColumn("thuoc_canh_bao", F.lit(True))
    sample = silver_df.sample(fraction=0.05, seed=2022).select(*article_cols).withColumn("thuoc_canh_bao", F.lit(False))
    articles = in_alert.unionByName(sample).dropDuplicates(["id"]).orderBy(F.rand(7)).limit(5000)
    articles.coalesce(1).write.mode("overwrite").parquet(P.GOLD_ARTICLES)
    print(f"-> Ghi bảng bài viết tiêu biểu: {P.GOLD_ARTICLES}")

    # 5. HIỂN THỊ KẾT QUẢ MẪU ĐỘT BIẾN TRUYỀN THÔNG
    print("\n--- TOP CÁC SỰ KIỆN ĐỘT BIẾN / KHỦNG HOẢNG TRUYỀN THÔNG (Z-SCORE >= 1.5) ---")
    alert_events = anomaly_df.filter(F.col("muc_do_canh_bao") != "Bình thường") \
        .orderBy(F.col("z_score_crisis").desc(), F.col("z_score_volume").desc())
    
    total_alerts = alert_events.count()
    print(f"-> Tổng số sự kiện chạm ngưỡng cảnh báo: {total_alerts} sự kiện.")
    alert_events.show(15, truncate=False)

    print("\n--- MẪU 5 DÒNG DỮ LIỆU XU HƯỚNG TẦNG GOLD (BẢNG 1) ---")
    trend_df.show(5, truncate=False)

    total_time = time.time() - start_time
    print("=" * 75)
    print(f" PIPELINE TẦNG GOLD HOÀN TẤT THÀNH CÔNG TRONG {total_time:.2f} GIÂY")
    print(" Các bảng Gold hiện sẵn sàng kết nối API FastAPI và Web Dashboard!")
    print("=" * 75)

    spark.stop()


if __name__ == "__main__":
    main()
