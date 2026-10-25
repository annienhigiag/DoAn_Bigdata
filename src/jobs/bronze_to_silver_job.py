import os
import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.nlp.nlp_cleaner import clean_text_spark
from src.dedup.dedup import drop_exact_duplicates, drop_near_duplicates
from src.utils import lakehouse_paths as P


def create_spark_session():
    return SparkSession.builder \
        .appName("Lakehouse_ETL_Bronze_to_Silver") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def pct(part, total):
    return round(part / total * 100, 2) if total else 0.0


def main():
    start_time = time.time()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_path = os.environ.get("BRONZE_PATH", P.BRONZE_NEWS)
    silver_path = os.environ.get("SILVER_PATH", P.SILVER_CLEAN)
    stats_path = os.environ.get("ETL_STATS_PATH", P.GOLD_ETL_STATS)
    quarantine_path = os.environ.get("QUARANTINE_PATH", P.QUARANTINE)
    use_nfc = os.environ.get("ETL_USE_NFC", "1") == "1"

    print("=" * 60)
    print(" BẮT ĐẦU PIPELINE ETL: BRONZE -> SILVER")
    print(f" Nguồn : {bronze_path}")
    print(f" Đích  : {silver_path}")
    print("=" * 60)

    # 1. ĐỌC DỮ LIỆU TẦNG BRONZE
    print("\n[Bước 1/6] Đang đọc dữ liệu Bronze từ HDFS...")
    raw_df = spark.read.parquet(bronze_path)
    initial_count = raw_df.count()
    print(f"-> Tổng số bản ghi thô (Bronze): {initial_count:,}")

    # 2. LÀM SẠCH VĂN BẢN: loại HTML, chuẩn hóa Unicode NFC, gộp khoảng trắng
    print("\n[Bước 2/6] Làm sạch văn bản (HTML, Unicode NFC, khoảng trắng)...")
    df = clean_text_spark(raw_df, "title", "title_clean", use_nfc=use_nfc)
    df = clean_text_spark(df, "content", "content_clean", use_nfc=use_nfc)
    df = df \
        .withColumn("source", F.coalesce(F.trim(F.lower(F.col("source"))), F.lit("unknown"))) \
        .withColumn("topic", F.coalesce(F.trim(F.col("topic")), F.lit("Khác"))) \
        .withColumn("crawled_date", F.to_date(F.col("crawled_at")))

    # 3. KIỂM TRA CHẤT LƯỢNG: bản ghi không hợp lệ đưa vào _quarantine
    print("\n[Bước 3/6] Kiểm tra chất lượng dữ liệu...")
    is_valid = (
        F.col("title_clean").isNotNull() & (F.length("title_clean") > 5)
        & F.col("content_clean").isNotNull() & (F.length("content_clean") >= 50)
        & F.col("crawled_at").isNotNull()
        & F.col("url").isNotNull()
    )
    df = df.withColumn("_valid", is_valid).cache()
    cleaned_df = df.filter("_valid").drop("_valid")
    quarantine_df = df.filter(~F.col("_valid")).drop("_valid")

    valid_count = cleaned_df.count()
    invalid_count = initial_count - valid_count
    print(f"-> Bản ghi hợp lệ: {valid_count:,}")
    print(f"-> Bản ghi bị loại (rỗng/quá ngắn/thiếu ngày): {invalid_count:,} -> {quarantine_path}")
    quarantine_df.write.mode("overwrite").parquet(quarantine_path)

    # 4. KHỬ TRÙNG LẶP TẦNG 1: EXACT (URL + SHA-256 nội dung)
    print("\n[Bước 4/6] Khử trùng lặp chính xác (URL, SHA-256 nội dung)...")
    exact_df, exact_stats = drop_exact_duplicates(
        cleaned_df, url_col="url", title_col="title_clean", content_col="content_clean"
    )
    print(f"-> Trùng URL: {exact_stats['exact_url_removed']:,} | Trùng nội dung: {exact_stats['exact_content_removed']:,}")

    # 5. KHỬ TRÙNG LẶP TẦNG 2: SIMHASH 64-BIT, HAMMING <= 3
    print("\n[Bước 5/6] Khử near-duplicate bằng SimHash 64-bit (Hamming <= 3)...")
    threshold = int(os.environ.get("SIMHASH_HAMMING", "3"))
    dedup_df, near_stats, _ = drop_near_duplicates(
        exact_df, key_col="url", text_col="content_clean", order_cols=("crawled_at",), threshold=threshold
    )
    final_count = dedup_df.count()
    print(f"-> Cặp near-duplicate: {near_stats['near_dup_pairs']:,} | Bài bị loại: {near_stats['near_dup_removed']:,}")

    duplicate_total = exact_stats["exact_url_removed"] + exact_stats["exact_content_removed"] + near_stats["near_dup_removed"]

    final_silver_df = dedup_df.select(
        F.col("id"),
        F.col("url"),
        F.col("source"),
        F.col("topic"),
        F.col("author"),
        F.col("title_clean").alias("title"),
        F.col("content_clean").alias("content"),
        F.col("picture_count"),
        F.col("simhash"),
        F.col("crawled_at"),
        F.col("crawled_date"),
    )

    # 6. GHI SILVER PHÂN VÙNG THEO NGÀY + BÁO CÁO CHẤT LƯỢNG ETL
    print(f"\n[Bước 6/6] Ghi Silver phân vùng theo 'crawled_date' vào {silver_path}...")
    final_silver_df.write \
        .mode("overwrite") \
        .partitionBy("crawled_date") \
        .parquet(silver_path)

    stats = {
        "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "bronze_records": initial_count,
        "invalid_records": invalid_count,
        "valid_records": valid_count,
        "exact_url_duplicates": exact_stats["exact_url_removed"],
        "exact_content_duplicates": exact_stats["exact_content_removed"],
        "near_duplicate_pairs": near_stats["near_dup_pairs"],
        "near_duplicates": near_stats["near_dup_removed"],
        "hamming_threshold": threshold,
        "duplicates_total": duplicate_total,
        "duplicate_rate_pct": pct(duplicate_total, valid_count),
        "invalid_rate_pct": pct(invalid_count, initial_count),
        "silver_records": final_count,
        "silver_rate_pct": pct(final_count, initial_count),
    }
    # Tạo DataFrame 1 dòng bằng literal (không cần Python worker)
    stats_df = spark.range(1).select(*[F.lit(v).alias(k) for k, v in stats.items()])
    stats_df.coalesce(1).write.mode("overwrite").parquet(stats_path)

    print("\n--- BÁO CÁO TỶ LỆ TRÙNG LẶP ---")
    print(f" Bronze                    : {initial_count:,}")
    print(f" Không hợp lệ (quarantine) : {invalid_count:,} ({stats['invalid_rate_pct']}%)")
    print(f" Trùng URL                 : {stats['exact_url_duplicates']:,}")
    print(f" Trùng nội dung (SHA-256)  : {stats['exact_content_duplicates']:,}")
    print(f" Near-duplicate (SimHash)  : {stats['near_duplicates']:,}")
    print(f" Tỷ lệ trùng lặp đã lọc    : {duplicate_total:,} / {valid_count:,} = {stats['duplicate_rate_pct']}%")
    print(f" Silver                    : {final_count:,} ({stats['silver_rate_pct']}% của Bronze)")
    print(f"-> Đã lưu báo cáo vào: {stats_path}")

    print("\n--- THỐNG KÊ PHÂN BỐ BẢN GHI THEO NGÀY (TOP 10 NGÀY) ---")
    final_silver_df.groupBy("crawled_date").count().orderBy(F.col("crawled_date").desc()).show(10, truncate=False)

    total_time = time.time() - start_time
    print("=" * 60)
    print(f" PIPELINE HOÀN TẤT THÀNH CÔNG TRONG {total_time:.2f} GIÂY")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
