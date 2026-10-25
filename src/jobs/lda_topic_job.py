"""
Mô hình chủ đề LDA (Latent Dirichlet Allocation) bằng Spark MLlib trên tầng Silver.

Pipeline: tiêu đề + sapo -> tách từ pyvi -> RegexTokenizer -> StopWordsRemover
          -> CountVectorizer -> LDA (k chủ đề)
Kết quả:
  gold/lda_chu_de.parquet           : mỗi chủ đề + 12 từ khóa nặng ký nhất
  gold/lda_chu_de_theo_ngay.parquet : số bài theo ngày x chủ đề trội (xu hướng chủ đề)
  models/lda_model                  : PipelineModel để gán chủ đề cho bài mới
"""
import os
import sys
import time

from pyspark.ml import Pipeline
from pyspark.ml.clustering import LDA
from pyspark.ml.feature import CountVectorizer, RegexTokenizer, StopWordsRemover
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.models.sentiment_features import prepare_text
from src.nlp.stopwords import VIETNAMESE_STOPWORDS
from src.utils import lakehouse_paths as P

NUM_TOPICS = int(os.environ.get("LDA_TOPICS", "12"))
TOP_TERMS = 12

# Từ dừng bổ sung cho mô hình chủ đề: hư từ, từ báo chí lặp lại không mang chủ đề
LDA_EXTRA_STOPWORDS = [
    "không", "này", "đó", "rằng", "hơn", "rất", "đã", "anh", "chị", "em", "ông", "bà", "họ", "tôi",
    "chúng_tôi", "chúng_ta", "mình", "bạn", "người", "năm", "ngày", "tháng", "giờ", "lần", "thì", "là",
    "có_thể", "cho_biết", "theo", "sau_khi", "trước", "khi", "nhiều", "các", "những", "đến", "trong",
    "ngoài", "vừa", "mới", "đang", "sẽ", "được", "bị", "tại", "hiện", "hiện_nay", "cũng", "nhưng",
    "nếu", "vì", "nên", "để", "với", "và", "hoặc", "của", "cùng", "từ", "về", "ra", "vào", "lên",
    "xuống", "lại", "đi", "một", "hai", "ba", "thêm", "nhất", "như", "gì", "nào", "ai", "đây", "kia",
    "việc", "điều", "thông_tin", "trường_hợp", "hôm_nay", "sáng", "chiều", "tối", "ảnh", "video",
    "clip", "tp", "hcm", "hà_nội", "tin", "bài", "phóng_viên", "báo", "đồng", "triệu", "tỷ",
    "http", "https", "www", "com", "vn", "btv", "con", "cô", "còn", "tới", "sự", "biết", "làm", "nói",
    "đưa", "cuộc", "số", "so", "mức", "xin", "thu", "nguồn", "chiếc", "xảy", "dùng", "sử_dụng",
]


def create_spark_session():
    return SparkSession.builder \
        .appName("Lakehouse_LDA_Topic_Modeling") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def main():
    start = time.time()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print(f" MÔ HÌNH CHỦ ĐỀ LDA (k = {NUM_TOPICS}) TRÊN TẦNG SILVER")
    print("=" * 70)

    silver = spark.read.parquet(P.SILVER_SENTIMENT).select("id", "title", "content", "crawled_date", "sentiment_label")
    docs = silver.withColumn("text", F.concat_ws(" . ", "title", F.substring("content", 1, 600)))
    docs = prepare_text(docs).cache()
    print(f"-> Số bài: {docs.count():,}")

    pipeline = Pipeline(stages=[
        RegexTokenizer(inputCol="text_tok", outputCol="tokens", pattern=r"[^\p{L}_]+", minTokenLength=2, toLowercase=True),
        StopWordsRemover(inputCol="tokens", outputCol="words",
                         stopWords=sorted(set(VIETNAMESE_STOPWORDS + LDA_EXTRA_STOPWORDS))),
        CountVectorizer(inputCol="words", outputCol="features", vocabSize=8000, minDF=15, maxDF=0.3),
        LDA(k=NUM_TOPICS, maxIter=30, optimizer="online", seed=2022, topicDistributionCol="topic_dist"),
    ])
    print("\n[1/3] Huấn luyện LDA...")
    model = pipeline.fit(docs)
    vocab = model.stages[2].vocabulary
    lda_model = model.stages[3]
    print(f"-> Từ vựng: {len(vocab):,} từ | log-perplexity: {lda_model.logPerplexity(model.transform(docs).select('features')):.3f}")

    print("\n[2/3] Trích từ khóa cho từng chủ đề...")
    topics = lda_model.describeTopics(TOP_TERMS).collect()
    rows = []
    for t in topics:
        terms = [vocab[i].replace("_", " ") for i in t["termIndices"]]
        weights = [round(float(w), 5) for w in t["termWeights"]]
        rows.append((int(t["topic"]), ", ".join(terms[:4]), ", ".join(terms), weights))
        print(f"   Chủ đề {t['topic']:>2}: {', '.join(terms)}")

    topic_df = spark.range(1).select(F.explode(F.array(*[
        F.struct(F.lit(tid).alias("topic_id"), F.lit(label).alias("nhan_chu_de"),
                 F.lit(terms).alias("tu_khoa"), F.array(*[F.lit(w) for w in weights]).alias("trong_so"))
        for tid, label, terms, weights in rows
    ])).alias("r")).select("r.*")

    print("\n[3/3] Gán chủ đề trội cho từng bài & tổng hợp theo ngày...")
    assigned = model.transform(docs) \
        .withColumn("dist", vector_to_array("topic_dist")) \
        .withColumn("topic_prob", F.array_max("dist")) \
        .withColumn("topic_id", F.expr("array_position(dist, topic_prob)").cast("int") - 1)

    by_day = assigned.groupBy("crawled_date", "topic_id").agg(
        F.count("*").alias("so_bai"),
        F.sum(F.when(F.col("sentiment_label") == "Tiêu cực", 1).otherwise(0)).alias("so_bai_tieu_cuc"),
        F.round(F.avg("topic_prob"), 4).alias("xac_suat_tb"),
    ).join(topic_df.select("topic_id", "nhan_chu_de"), "topic_id")

    topic_sizes = assigned.groupBy("topic_id").count().withColumnRenamed("count", "tong_so_bai")
    topic_df = topic_df.join(topic_sizes, "topic_id", "left").orderBy("topic_id")

    topic_df.coalesce(1).write.mode("overwrite").parquet(P.GOLD_LDA_TOPICS)
    by_day.write.mode("overwrite").parquet(P.GOLD_LDA_DAILY)
    model.write().overwrite().save(P.MODEL_LDA)

    topic_df.select("topic_id", "tong_so_bai", "nhan_chu_de").show(NUM_TOPICS, truncate=False)
    print(f"-> Ghi {P.GOLD_LDA_TOPICS}, {P.GOLD_LDA_DAILY}, model {P.MODEL_LDA}")
    print(f" HOÀN TẤT LDA TRONG {time.time() - start:.1f} GIÂY")
    spark.stop()


if __name__ == "__main__":
    main()
