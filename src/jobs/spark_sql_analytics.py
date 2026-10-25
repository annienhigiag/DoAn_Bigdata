import sys
import os
import time
from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.utils import lakehouse_paths as P


def create_spark_session():
    """
    Khởi tạo SparkSession tối ưu hóa cho các câu truy vấn Spark SQL.
    """
    return SparkSession.builder \
        .appName("Lakehouse_Step5_Spark_SQL_Analytics") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def main():
    start_time = time.time()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_path = P.SILVER_SENTIMENT
    output_path = f"{P.LAKEHOUSE_ROOT}/gold/spark_sql_summary.parquet"

    print(" TRUY VẤN VÀ PHÂN TÍCH DỮ LIỆU BẰNG SPARK SQL")
    print(f" Nguồn dữ liệu (Silver Table): {silver_path}")
    # 1. NẠP DỮ LIỆU TẦNG SILVER
    print("\n[1/5] Đang nạp bảng Silver từ HDFS...")
    df = spark.read.parquet(silver_path)
    total_count = df.count()
    print(f"-> Đã nạp thành công: {total_count:,} bài báo.")

    # 2. ĐĂNG KÝ BẢNG TẠM SPARK SQL (TEMP VIEW)
    print("\n[2/5] Đăng ký Temporary View 'silver_news' cho Spark SQL Engine...")
    df.createOrReplaceTempView("silver_news")
    print("-> Đã tạo bảng ảo 'silver_news' sẵn sàng cho các câu lệnh SELECT.")

    # 3. THỰC THI CÁC TRUY VẤN SPARK SQL
    print("\n")
    print(" THỰC THI TRUY VẤN 1: TỔNG QUAN HỆ THỐNG (SPARK SQL OVERVIEW)")
    query_overview = """
        SELECT 
            COUNT(*) AS tong_so_bai,
            COUNT(DISTINCT crawled_date) AS so_ngay_khao_sat,
            COUNT(DISTINCT source) AS so_nguon_bao,
            COUNT(DISTINCT topic) AS so_chuyen_muc,
            ROUND(AVG(LENGTH(content)), 0) AS do_dai_noi_dung_tb
        FROM silver_news
    """
    spark.sql(query_overview).show(truncate=False)

    print("\n")
    print(" THỰC THI TRUY VẤN 2: TOP 10 NGUỒN BÁO CÓ SỐ LƯỢNG BÀI NHIỀU NHẤT")
    query_top_sources = """
        SELECT 
            source AS nguon_bao,
            COUNT(*) AS so_luong_bai,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM silver_news), 2) AS ty_le_phan_tram,
            ROUND(AVG(picture_count), 1) AS so_anh_trung_binh
        FROM silver_news
        GROUP BY source
        ORDER BY so_luong_bai DESC
        LIMIT 10
    """
    spark.sql(query_top_sources).show(truncate=False)

    print("\n")
    print(" THỰC THI TRUY VẤN 3: PHÂN BỐ CẢM XÚC THEO CHUYÊN MỤC (SENTIMENT BY TOPIC)")
    query_sentiment_by_topic = """
        SELECT 
            topic AS chuyen_muc,
            COUNT(*) AS tong_bai,
            SUM(CASE WHEN sentiment_label = 'Tích cực' THEN 1 ELSE 0 END) AS so_bai_tich_cuc,
            SUM(CASE WHEN sentiment_label = 'Trung tính' THEN 1 ELSE 0 END) AS so_bai_trung_tinh,
            SUM(CASE WHEN sentiment_label = 'Tiêu cực' THEN 1 ELSE 0 END) AS so_bai_tieu_cuc,
            ROUND(SUM(CASE WHEN sentiment_label = 'Tích cực' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS ty_le_tich_cuc_pct,
            ROUND(SUM(CASE WHEN sentiment_label = 'Tiêu cực' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS ty_le_tieu_cuc_pct
        FROM silver_news
        GROUP BY topic
        HAVING COUNT(*) >= 500
        ORDER BY tong_bai DESC
        LIMIT 10
    """
    spark.sql(query_sentiment_by_topic).show(truncate=False)

    print("\n")
    print(" THỰC THI TRUY VẤN 4: TOP 5 NGÀY CÓ MẬT ĐỘ TIN TỨC CAO NHẤT (TRENDING DATES)")
    query_trending_dates = """
        SELECT 
            crawled_date AS ngay,
            COUNT(*) AS so_bai_viet,
            ROUND(AVG(sentiment_score), 4) AS diem_tin_cay_tb
        FROM silver_news
        GROUP BY crawled_date
        ORDER BY so_bai_viet DESC
        LIMIT 5
    """
    spark.sql(query_trending_dates).show(truncate=False)

    # 4. GHI KẾT QUẢ PHÂN TÍCH SPARK SQL VÀO TẦNG GOLD TRÊN HDFS
    print(f"\n[4/5] Đang lưu bảng tổng hợp Spark SQL vào HDFS Gold ({output_path})...")
    sql_summary_df = spark.sql(query_sentiment_by_topic)
    sql_summary_df.write \
        .mode("overwrite") \
        .parquet(output_path)
    print("-> Đã lưu bảng phân tích Spark SQL thành công!")

    total_time = time.time() - start_time
    print(f" HOÀN TẤT SPARK SQL XUẤT SẮC TRONG {total_time:.2f} GIÂY")

    spark.stop()


if __name__ == "__main__":
    main()
