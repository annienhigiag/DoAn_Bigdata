"""
Phát hiện đột biến theo THỰC THỂ / THƯƠNG HIỆU (người, tổ chức, địa danh, nhãn hàng).

Trích thực thể từ tiêu đề bằng 2 cách, chạy hoàn toàn bằng hàm nội tại Spark:
  1. Cụm từ viết hoa liên tiếp (2-4 âm tiết), ví dụ "Quang Hải", "Hà Nội", "Bộ Y"
     (bỏ âm tiết đầu tiêu đề vì chữ đầu câu luôn viết hoa).
  2. Từ điển thương hiệu/tổ chức một từ (Vingroup, Apple, Viettel, ...).

Với mỗi thực thể có >= MIN_MENTIONS bài, dựng lưới ngày x thực thể (ngày không nhắc = 0),
tính Z-score số bài theo ngày; đột biến khi Z >= 3 và có ít nhất 5 bài trong ngày.
Kết quả:
  gold/canh_bao_thuc_the.parquet : ngày, thực thể, số bài, số bài tiêu cực, Z-score, mức cảnh báo
  gold/thuc_the_noi_bat.parquet  : tổng hợp theo thực thể (tổng bài, tỷ lệ tiêu cực, Z cao nhất)
"""
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.utils import lakehouse_paths as P

MIN_MENTIONS = int(os.environ.get("ENTITY_MIN_MENTIONS", "15"))

# Cụm 2-4 từ viết hoa chữ cái đầu (hỗ trợ chữ có dấu tiếng Việt)
CAPITALIZED_PHRASE = r"\p{Lu}[\p{L}\p{M}]*(?:\s+\p{Lu}[\p{L}\p{M}]*){1,3}"

SINGLE_WORD_BRANDS = [
    "Vingroup", "VinFast", "Vinamilk", "Viettel", "VNPT", "Vietcombank", "BIDV", "Agribank", "Techcombank",
    "VPBank", "MB", "Vietjet", "Bamboo", "Sabeco", "Masan", "FPT", "Hòa Phát", "Petrolimex", "PVN", "EVN",
    "Apple", "iPhone", "Samsung", "Google", "Facebook", "Meta", "TikTok", "Shopee", "Lazada", "Tiki", "Grab",
    "Tesla", "Microsoft", "Amazon", "Netflix", "Twitter", "Xiaomi", "Huawei", "Toyota", "Honda", "Hyundai",
    "BTS", "Blackpink", "Covid", "Ukraine", "Nga", "Mỹ", "Trung Quốc", "NATO", "WHO", "ASEAN", "EU",
    "MU", "Chelsea", "Liverpool", "Arsenal", "Barcelona", "Ronaldo", "Messi", "SEA Games", "V-League",
]

# Cụm viết hoa không phải thực thể (đầu mục, từ tiêu đề chung)
NOT_ENTITY = ["Video", "Clip", "Ảnh", "Infographic", "TRỰC TIẾP", "Trực Tiếp", "Tin Tức", "Kết Quả", "Xổ Số",
              "Lịch Thi Đấu", "XSMN", "XSMB", "XSMT", "Tiếng Anh",
              # tên báo xuất hiện trong tiêu đề (không phải chủ thể của tin)
              "Báo Dân", "Tuổi Trẻ Online", "Thanh Niên", "Lao Động", "Dân Việt", "Zing News"]


def create_spark_session():
    return SparkSession.builder \
        .appName("Lakehouse_Entity_Trend_Anomaly") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def main():
    start = time.time()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 70)
    print(" ĐỘT BIẾN THEO THỰC THỂ / THƯƠNG HIỆU (Z-SCORE)")
    print("=" * 70)

    silver = spark.read.parquet(P.SILVER_SENTIMENT).select("id", "title", "crawled_date", "sentiment_label", "source")

    # Bỏ âm tiết đầu tiêu đề (luôn viết hoa) rồi tìm cụm viết hoa
    title_body = F.regexp_replace(F.col("title"), r"^\S+\s*", "")
    phrases = F.regexp_extract_all(title_body, F.lit(CAPITALIZED_PHRASE), F.lit(0))
    brand_hits = F.filter(
        F.array(*[F.lit(b) for b in SINGLE_WORD_BRANDS]),
        lambda b: F.col("_title_pad").contains(F.concat(F.lit(" "), b, F.lit(" ")))
    )

    mentions = silver \
        .withColumn("_title_pad", F.concat(F.lit(" "), F.regexp_replace("title", r"[^\p{L}\p{M}\p{N}\-]+", " "), F.lit(" "))) \
        .withColumn("entities", F.array_union(phrases, brand_hits)) \
        .select("id", "crawled_date", "sentiment_label", F.explode("entities").alias("entity")) \
        .withColumn("entity", F.trim("entity")) \
        .filter(~F.col("entity").isin(NOT_ENTITY) & (F.length("entity") >= 2)) \
        .dropDuplicates(["id", "entity"]) \
        .cache()

    totals = mentions.groupBy("entity").agg(
        F.count("*").alias("tong_so_bai"),
        F.sum(F.when(F.col("sentiment_label") == "Tiêu cực", 1).otherwise(0)).alias("tong_tieu_cuc"),
    ).filter(F.col("tong_so_bai") >= MIN_MENTIONS)
    print(f"-> Số thực thể có >= {MIN_MENTIONS} bài: {totals.count():,}")

    daily = mentions.join(totals.select("entity"), "entity").groupBy("crawled_date", "entity").agg(
        F.count("*").alias("so_bai"),
        F.sum(F.when(F.col("sentiment_label") == "Tiêu cực", 1).otherwise(0)).alias("so_bai_tieu_cuc"),
    )

    # Lưới đầy đủ ngày x thực thể để ngày không được nhắc tính là 0
    dates = silver.select("crawled_date").distinct()
    grid = dates.crossJoin(totals.select("entity")) \
        .join(daily, ["crawled_date", "entity"], "left") \
        .na.fill(0, ["so_bai", "so_bai_tieu_cuc"])

    w = Window.partitionBy("entity")
    alerts = grid \
        .withColumn("tb_so_bai", F.avg("so_bai").over(w)) \
        .withColumn("do_lech_chuan", F.stddev("so_bai").over(w)) \
        .withColumn("z_score", F.when(F.col("do_lech_chuan") > 0,
                                      F.round((F.col("so_bai") - F.col("tb_so_bai")) / F.col("do_lech_chuan"), 2))
                    .otherwise(F.lit(0.0))) \
        .withColumn("ty_le_tieu_cuc", F.round(F.when(F.col("so_bai") > 0, F.col("so_bai_tieu_cuc") / F.col("so_bai") * 100)
                                              .otherwise(F.lit(0.0)), 1)) \
        .withColumn("muc_do_canh_bao",
                    F.when((F.col("z_score") >= 3) & (F.col("so_bai") >= 5), "Đột biến mạnh")
                     .when((F.col("z_score") >= 2) & (F.col("so_bai") >= 3), "Tăng cao")
                     .otherwise("Bình thường")) \
        .withColumn("tb_so_bai", F.round("tb_so_bai", 2)) \
        .withColumn("do_lech_chuan", F.round("do_lech_chuan", 2)) \
        .cache()

    top = alerts.groupBy("entity").agg(
        F.sum("so_bai").alias("tong_so_bai"),
        F.sum("so_bai_tieu_cuc").alias("tong_tieu_cuc"),
        F.max("z_score").alias("z_max"),
        F.sum(F.when(F.col("muc_do_canh_bao") != "Bình thường", 1).otherwise(0)).alias("so_ngay_canh_bao"),
    ).withColumn("ty_le_tieu_cuc", F.round(F.col("tong_tieu_cuc") / F.col("tong_so_bai") * 100, 1)) \
        .orderBy(F.desc("tong_so_bai"))

    alerts.write.mode("overwrite").partitionBy("crawled_date").parquet(P.GOLD_ENTITY_ALERT)
    top.coalesce(1).write.mode("overwrite").parquet(P.GOLD_ENTITY_TOP)

    print("\n--- TOP 15 THỰC THỂ ĐƯỢC NHẮC NHIỀU NHẤT ---")
    top.show(15, truncate=False)
    print("--- CÁC ĐỘT BIẾN MẠNH NHẤT ---")
    alerts.filter(F.col("muc_do_canh_bao") != "Bình thường").orderBy(F.desc("z_score")) \
        .select("crawled_date", "entity", "so_bai", "tb_so_bai", "z_score", "ty_le_tieu_cuc", "muc_do_canh_bao") \
        .show(15, truncate=False)
    print(f" HOÀN TẤT TRONG {time.time() - start:.1f} GIÂY")
    spark.stop()


if __name__ == "__main__":
    main()
