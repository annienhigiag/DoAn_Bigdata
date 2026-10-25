"""
Xuất mẫu ngẫu nhiên 200 bài báo tầng Silver ra CSV để nhóm gán nhãn cảm xúc bằng tay.

Quy trình:
  1. Chạy job này -> hdfs:///lakehouse/bd05_tin_tuc/labels/manual_200_sample.csv
  2. Lấy file về máy, điền cột label (0 = Tiêu cực, 1 = Trung tính, 2 = Tích cực)
  3. Lưu thành manual_200.csv (giữ 2 cột id,label là đủ) và đưa lên
     hdfs:///lakehouse/bd05_tin_tuc/labels/manual_200.csv
  4. Chạy lại src/models/train_sentiment_mllib.py -> có thêm chỉ số trên "manual_200" (domain shift)
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.models.sentiment_features import SAPO_CHARS
from src.utils import lakehouse_paths as P


def main():
    spark = SparkSession.builder.appName("Export_Manual_Label_Sample").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    n = int(os.environ.get("SAMPLE_SIZE", "200"))
    silver = spark.read.parquet(P.SILVER_CLEAN)
    sample = silver.orderBy(F.rand(seed=2022)).limit(n).select(
        "id", "source", "topic", "crawled_date", "title",
        F.substring("content", 1, SAPO_CHARS).alias("sapo"),
        "url",
        F.lit("").alias("label"),
    )
    sample.coalesce(1).write.mode("overwrite").option("header", True).csv(P.LABELS_MANUAL_SAMPLE)
    print(f"-> Đã xuất {n} bài vào {P.LABELS_MANUAL_SAMPLE}")
    print("   Lấy về máy: docker exec namenode hdfs dfs -getmerge "
          "/lakehouse/bd05_tin_tuc/labels/manual_200_sample.csv /tmp/manual_200_sample.csv")
    spark.stop()


if __name__ == "__main__":
    main()
