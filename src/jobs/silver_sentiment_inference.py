import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.models.sentiment_features import build_article_text, prepare_text
from src.utils import lakehouse_paths as P


def create_spark_session():
    """
    Tạo SparkSession tối ưu cho job batch inference quy mô 50.000+ bản ghi.
    """
    return SparkSession.builder \
        .appName("Lakehouse_Silver_Sentiment_Inference") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def main():
    start_time = time.time()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_input_path = os.environ.get("SILVER_PATH", P.SILVER_CLEAN)
    model_hdfs_path = os.environ.get("MODEL_PATH", P.MODEL_SENTIMENT)
    silver_output_path = os.environ.get("SILVER_SENTIMENT_PATH", P.SILVER_SENTIMENT)

    print("=" * 70)
    print(" BẮT ĐẦU BATCH INFERENCE CẢM XÚC TẦNG SILVER")
    print(f" Nguồn vào   : {silver_input_path}")
    print(f" Mô hình HDFS: {model_hdfs_path}")
    print(f" Đích ghi    : {silver_output_path}")
    print("=" * 70)

    # 1. ĐỌC DỮ LIỆU TẦNG SILVER SẠCH TỪ HDFS
    print("\n[Bước 1/5] Đang nạp dữ liệu sạch tầng Silver từ HDFS...")
    silver_df = spark.read.parquet(silver_input_path)
    total_articles = silver_df.count()
    print(f"-> Tổng số bài báo sạch cần gán nhãn: {total_articles:,} bài")

    # 2. NẠP PIPELINEMODEL TỪ HDFS
    print(f"\n[Bước 2/5] Đang nạp PipelineModel đã huấn luyện từ HDFS...")
    model = PipelineModel.load(model_hdfs_path)
    print("-> Đã tải mô hình Spark MLlib thành công!")

    # 3. CHUẨN BỊ TRƯỜNG VĂN BẢN ĐẠI DIỆN
    print("\n[Bước 3/5] Chuẩn bị trường văn bản ('text') từ tiêu đề và đoạn đầu nội dung...")
    # Dùng chung hàm với bước huấn luyện để văn bản được biến đổi giống hệt nhau
    infer_prep_df = prepare_text(build_article_text(silver_df))

    # 4. THỰC THI TRANSFORM PHÂN TÁN
    print("\n[Bước 4/5] Đang thực thi phân tán dự đoán cảm xúc (PipelineModel.transform)...")
    pred_df = model.transform(infer_prep_df)

    labeled_df = pred_df \
        .withColumn(
            "sentiment_label",
            F.when(F.col("prediction") == 0, "Tiêu cực")
             .when(F.col("prediction") == 1, "Trung tính")
             .when(F.col("prediction") == 2, "Tích cực")
             .otherwise("Không xác định")
        ) \
        .withColumn("sentiment_score", F.round(F.array_max(vector_to_array(F.col("probability"))), 4)) \
        .withColumn("sentiment_class", F.col("prediction").cast("int"))

    final_sentiment_df = labeled_df.select(
        F.col("id"),
        F.col("url"),
        F.col("source"),
        F.col("topic"),
        F.col("author"),
        F.col("title"),
        F.col("content"),
        F.col("picture_count"),
        F.col("sentiment_class"),
        F.col("sentiment_label"),
        F.col("sentiment_score"),
        F.col("crawled_at"),
        F.col("crawled_date")
    )

    # 5. GHI DỮ LIỆU ĐÃ GÁN NHÃN VÀO HDFS THEO PHÂN VÙNG NGÀY
    print(f"\n[Bước 5/5] Đang ghi dữ liệu gán nhãn ra HDFS: {silver_output_path}...")
    final_sentiment_df.write \
        .mode("overwrite") \
        .partitionBy("crawled_date") \
        .parquet(silver_output_path)

    print("-> Đã hoàn thành ghi bảng gán nhãn lên HDFS!")

    # 6. THỐNG KÊ PHÂN BỐ CẢM XÚC THỰC TẾ
    print("\n--- PHÂN BỐ CẢM XÚC TRÊN TOÀN BỘ DỮ LIỆU TẦNG SILVER ---")
    sentiment_dist = final_sentiment_df.groupBy("sentiment_label") \
        .agg(
            F.count("*").alias("so_bai"),
            F.round(F.count("*") / total_articles * 100, 2).alias("ty_le_phan_tram"),
            F.round(F.avg("sentiment_score"), 4).alias("diem_tin_cay_tb")
        ) \
        .orderBy(F.col("so_bai").desc())
    sentiment_dist.show(truncate=False)

    print("\n--- PHÂN BỐ CẢM XÚC THEO CHUYÊN MỤC BÁO CHÍ (TOP 5) ---")
    topic_dist = final_sentiment_df.groupBy("topic", "sentiment_label") \
        .count() \
        .orderBy(F.col("count").desc())
    topic_dist.show(15, truncate=False)

    total_time = time.time() - start_time
    print("=" * 70)
    print(f" BATCH INFERENCE HOÀN TẤT THÀNH CÔNG TRONG {total_time:.2f} GIÂY")
    print(f" Tốc độ xử lý trung bình: {total_articles / total_time:.1f} bài/giây")
    print(" Dữ liệu gán nhãn sẵn sàng cho tầng Gold và API Dashboard!")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
