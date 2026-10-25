import os
import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Đảm bảo import được các module trong src/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.nlp.stopwords import VIETNAMESE_STOPWORDS
from src.models.sentiment_features import LABEL_NAMES, build_article_text, prepare_text
from src.utils import lakehouse_paths as P
from src.utils.spark_helper import path_exists

REPORTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../reports"))


def create_spark_session():
    """
    Tạo SparkSession tối ưu cho huấn luyện MLlib.
    """
    return SparkSession.builder         .appName("Train_Spark_MLlib_Sentiment")         .config("spark.sql.parquet.compression.codec", "snappy")         .config("spark.sql.adaptive.enabled", "true")         .getOrCreate()


def get_seed_dataset(spark):
    """
    Bộ câu mẫu tự soạn 3 lớp (0: Tiêu cực, 1: Trung tính, 2: Tích cực), chỉ dùng khi chưa có
    tập nhãn thật trên HDFS. Không nhân bản dữ liệu: mỗi câu xuất hiện đúng 1 lần, nên chỉ số
    đo được trên tập test là trung thực (nhưng tập test rất nhỏ).
    """
    sample_corpus = [
        # --- 0: Tiêu cực (Negative) ---
        ("Doanh nghiệp phá sản hàng loạt do chi phí sản xuất tăng phi mã", 0),
        ("Thị trường chứng khoán giảm điểm sâu kỷ lục, nhà đầu tư thua lỗ nặng nề", 0),
        ("Giá xăng dầu tiếp tục tăng cao gây áp lực lạm phát trầm trọng", 0),
        ("Dịch bệnh bùng phát phức tạp khiến hoạt động kinh doanh bị đình trệ kéo dài", 0),
        ("Sai phạm nghiêm trọng trong đấu thầu thiết bị y tế bị khởi tố bắt tạm giam", 0),
        ("Hàng ngàn lao động mất việc làm do công ty cắt giảm đơn hàng hàng loạt", 0),
        ("Ách tắc giao thông nghiêm trọng và ô nhiễm không khí ở mức báo động", 0),
        ("Thiệt hại nặng nề sau cơn bão lũ lớn, nhiều ngôi nhà bị sập và ngập sâu", 0),
        ("Lừa đảo trực tuyến gia tăng mạnh chiếm đoạt tiền gửi ngân hàng của người dân", 0),
        ("Áp lực nợ xấu ngân hàng gia tăng đáng báo động trong quý này", 0),
        ("Xuất khẩu sụt giảm nghiêm trọng do suy thoái kinh tế toàn cầu và đứt gãy cung ứng", 0),
        ("Tai nạn giao thông thảm khốc trên tuyến quốc lộ khiến nhiều người thương vong", 0),
        ("Chi phí sinh hoạt đắt đỏ đè nặng lên vai người có thu nhập thấp", 0),
        ("Doanh số bán lẻ tụt dốc thảm hại, sức mua thị trường ảm đạm", 0),
        ("Tình trạng quá tải bệnh viện diễn biến xấu gây bức xúc dư luận", 0),

        # --- 1: Trung tính (Neutral) ---
        ("Hội nghị sơ kết công tác 6 tháng đầu năm được tổ chức tại Hà Nội", 1),
        ("Bộ Y tế ban hành hướng dẫn mới về tiêm chủng phòng dịch bệnh", 1),
        ("Ngân hàng Nhà nước công bố tỷ giá trung tâm ngày hôm nay", 1),
        ("Tổng cục Thống kê báo cáo chỉ số giá tiêu dùng CPI định kỳ tháng này", 1),
        ("Lịch thi tốt nghiệp trung học phổ thông quốc gia diễn ra vào cuối tháng 6", 1),
        ("Thời tiết các khu vực trên cả nước trong tuần này có mưa rào rải rác", 1),
        ("Ủy ban nhân dân thành phố họp thảo luận về quy hoạch sử dụng đất", 1),
        ("Bộ Giao thông Vận tải kiểm tra tiến độ dự án đường cao tốc Bắc Nam", 1),
        ("Đại học Quốc gia công bố điểm chuẩn kỳ thi đánh giá năng lực", 1),
        ("Chính phủ ban hành nghị định sửa đổi bổ sung về quản lý bất động sản", 1),
        ("Các đại biểu tham gia phiên chất vấn trực tiếp tại kỳ họp Quốc hội", 1),
        ("Bộ Công Thương theo dõi diễn biến cung cầu hàng hóa thiết yếu trên thị trường", 1),
        ("Giá vàng hôm nay đi ngang trong phiên giao dịch sáng đầu tuần", 1),
        ("Việt Nam và đối tác quốc tế trao đổi đoàn đại biểu cấp bộ ngành", 1),
        ("Thông báo tuyển dụng viên chức ngành giáo dục năm học mới", 1),

        # --- 2: Tích cực (Positive) ---
        ("Kinh tế Việt Nam phục hồi mạnh mẽ sau đại dịch vượt xa mọi dự báo", 2),
        ("Doanh thu du lịch tăng trưởng ấn tượng trong kỳ nghỉ lễ kích cầu thành công", 2),
        ("Kim ngạch xuất khẩu nông sản đạt kỷ lục mới nhờ mở rộng thị trường quốc tế", 2),
        ("Nhiều doanh nghiệp công nghệ Việt Nam ghi nhận lợi nhuận tăng trưởng vượt bậc", 2),
        ("Chính sách hỗ trợ kịp thời giúp người dân và doanh nghiệp nhanh chóng vượt khó", 2),
        ("Hạ tầng giao thông trọng điểm được khánh thành đưa vào sử dụng vượt tiến độ", 2),
        ("Thu hút dòng vốn đầu tư nước ngoài FDI khởi sắc mang lại hàng triệu việc làm mới", 2),
        ("Đội tuyển thể thao Việt Nam thi đấu xuất sắc giành trọn huy chương vàng", 2),
        ("Nhiều sáng kiến công nghệ xuất sắc mang lại hiệu quả kinh tế cao và giải thưởng lớn", 2),
        ("Đời sống người lao động được cải thiện đáng kể với mức thu nhập tăng cao", 2),
        ("Môi trường kinh doanh được cải thiện rõ rệt, thúc đẩy tinh thần khởi nghiệp", 2),
        ("Các bệnh nhân phục hồi sức khỏe hoàn toàn và xuất viện trong niềm vui lớn", 2),
        ("Sản lượng lương thực được mùa trúng giá đem lại niềm vui cho bà con nông dân", 2),
        ("Các đường bay quốc tế mở lại toàn diện tạo đà bứt phá cho ngành hàng không", 2),
        ("Chỉ số niềm tin kinh doanh tăng vọt báo hiệu làn sóng đầu tư mạnh mẽ", 2)
    ]

    schema = StructType([
        StructField("text", StringType(), False),
        StructField("label", IntegerType(), False)
    ])
    return spark.createDataFrame(sample_corpus, schema=schema)


def load_feedback_labels(spark):
    """
    Nhãn do người dùng sửa trên Dashboard (write-back, labels/feedback.csv: id,label,...)
    ghép với tiêu đề + sapo bài báo ở Silver -> thêm vào tập huấn luyện (dữ liệu đúng miền báo chí).
    """
    if not path_exists(spark, P.LABELS_FEEDBACK) or not path_exists(spark, P.SILVER_CLEAN):
        return None
    fb = spark.read.option("header", True).csv(P.LABELS_FEEDBACK)         .select(F.col("id").cast("long").alias("id"), F.col("label").cast("int").alias("label"), "updated_at")         .filter(F.col("label").isin(0, 1, 2))
    fb = fb.withColumn("_rn", F.row_number().over(Window.partitionBy("id").orderBy(F.desc("updated_at"))))         .filter("_rn = 1").select("id", "label")
    silver = spark.read.parquet(P.SILVER_CLEAN).select(F.col("id").cast("long").alias("id"), "title", "content")
    return build_article_text(fb.join(silver, "id")).select("text", "label")


def load_training_dataset(spark):
    """
    Ưu tiên tập nhãn thật trên HDFS (CSV có header: text,label), ví dụ UIT-VSFC
    (scripts/prepare_sentiment_labels.py). Nếu chưa có thì dùng bộ câu mẫu.
    Nhãn sửa tay từ Dashboard (write-back) luôn được cộng thêm nếu có.
    """
    labels_path = os.environ.get("LABELS_TRAIN_PATH", P.LABELS_TRAIN)
    if path_exists(spark, labels_path):
        df = spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(labels_path)
        df = df.select(F.col("text").cast("string"), F.col("label").cast("int"))             .filter(F.col("text").isNotNull() & F.col("label").isin(0, 1, 2))
        source = labels_path
    else:
        df, source = get_seed_dataset(spark), "seed_corpus (45 câu mẫu trong code)"

    feedback = load_feedback_labels(spark)
    if feedback is not None:
        n_fb = feedback.count()
        if n_fb:
            df = df.unionByName(feedback)
            source += f" + {n_fb} nhãn sửa tay từ Dashboard"
    return df, source


def add_class_weights(train_df):
    """Trọng số nghịch tần suất lớp để lớp hiếm (Trung tính) không bị lấn át -> cải thiện Macro-F1."""
    counts = {r["label"]: r["count"] for r in train_df.groupBy("label").count().collect()}
    total, k = sum(counts.values()), len(counts)
    weight = F.create_map(*[x for l, c in counts.items() for x in (F.lit(l), F.lit(total / (k * c)))])
    return train_df.withColumn("weight", weight[F.col("label")])


def stratified_split(df, train_ratio=0.8, seed=42):
    """
    Khử trùng lặp văn bản TRƯỚC khi chia, rồi chia phân tầng theo nhãn.
    Tập test không chứa câu nào đã có trong tập train (tránh rò rỉ dữ liệu).
    """
    df = df.dropDuplicates(["text"]).cache()
    labels = [r["label"] for r in df.select("label").distinct().collect()]
    train_df = df.sampleBy("label", fractions={l: train_ratio for l in labels}, seed=seed).cache()
    test_df = df.join(train_df.select("text"), "text", "left_anti").cache()
    return train_df, test_df


def build_pipeline():
    """
    Xây dựng Pipeline Spark MLlib phân tán:
    Tokenizer -> StopWordsRemover -> HashingTF -> IDF -> LogisticRegression
    Đầu vào là cột text_tok (đã tách từ bằng pyvi trong prepare_text).
    """
    tokenizer = RegexTokenizer(
        inputCol="text_tok",
        outputCol="raw_words",
        pattern=r"\s+",
        toLowercase=True
    )

    stop_words_remover = StopWordsRemover(
        inputCol="raw_words",
        outputCol="filtered_words",
        stopWords=VIETNAMESE_STOPWORDS
    )

    hashing_tf = HashingTF(
        inputCol="filtered_words",
        outputCol="raw_features",
        numFeatures=16384
    )

    idf = IDF(
        inputCol="raw_features",
        outputCol="features"
    )

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        probabilityCol="probability",
        family="multinomial",
        maxIter=50,
        regParam=0.01,
        elasticNetParam=0.0,
        weightCol="weight"
    )

    return Pipeline(stages=[tokenizer, stop_words_remover, hashing_tf, idf, lr])



def evaluate(predictions, dataset_name):
    """
    Tính Accuracy, Precision/Recall/F1 từng lớp, Macro (trung bình không trọng số) và Weighted.
    """
    predictions = predictions.cache()

    def ev(metric, label=None):
        e = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName=metric)
        if label is not None:
            e = e.setMetricLabel(float(label))
        return e.evaluate(predictions)

    present = sorted(r["label"] for r in predictions.select("label").distinct().collect())
    per_class = {}
    for l in present:
        per_class[l] = {
            "precision": ev("precisionByLabel", l),
            "recall": ev("recallByLabel", l),
            "f1": ev("fMeasureByLabel", l),
        }

    n = len(per_class) or 1
    metrics = {
        "dataset": dataset_name,
        "n_samples": predictions.count(),
        "accuracy": ev("accuracy"),
        "macro_precision": sum(c["precision"] for c in per_class.values()) / n,
        "macro_recall": sum(c["recall"] for c in per_class.values()) / n,
        "macro_f1": sum(c["f1"] for c in per_class.values()) / n,
        "weighted_f1": ev("f1"),
        "weighted_precision": ev("weightedPrecision"),
        "weighted_recall": ev("weightedRecall"),
    }

    confusion = predictions.groupBy("label") \
        .pivot("prediction", [0.0, 1.0, 2.0]).count() \
        .na.fill(0).orderBy("label")
    confusion_rows = confusion.collect()

    print("-" * 60)
    print(f" ĐÁNH GIÁ TRÊN: {dataset_name} ({metrics['n_samples']:,} mẫu)")
    print(f" Accuracy          : {metrics['accuracy'] * 100:.2f}%")
    print(f" Macro-Precision   : {metrics['macro_precision']:.4f}")
    print(f" Macro-Recall      : {metrics['macro_recall']:.4f}")
    print(f" Macro-F1          : {metrics['macro_f1']:.4f}")
    print(f" Weighted-F1       : {metrics['weighted_f1']:.4f}")
    for l, c in per_class.items():
        print(f"   Lớp {l} ({LABEL_NAMES[l]}): P={c['precision']:.4f} R={c['recall']:.4f} F1={c['f1']:.4f}")
    print(" Ma trận nhầm lẫn (hàng = nhãn thật, cột = dự đoán 0/1/2):")
    confusion.show(truncate=False)
    print("-" * 60)
    return metrics, per_class, confusion_rows


def evaluate_manual_labels(spark, model):
    """
    Đo domain shift: so mô hình với 200 bài báo Silver được nhóm gán nhãn tay
    (CSV có header: id,label). Bỏ qua nếu chưa có file.
    """
    manual_path = os.environ.get("LABELS_MANUAL_PATH", P.LABELS_MANUAL)
    silver_path = os.environ.get("SILVER_PATH", P.SILVER_CLEAN)
    if not path_exists(spark, manual_path) or not path_exists(spark, silver_path):
        print(f"-> Chưa có nhãn tay tại {manual_path}, bỏ qua đánh giá domain shift.")
        print("   Tạo mẫu 200 bài để gán nhãn bằng: src/jobs/export_manual_label_sample.py")
        return None

    labels = spark.read.option("header", True).csv(manual_path) \
        .select(F.col("id").cast("long").alias("id"), F.col("label").cast("int").alias("label")) \
        .filter(F.col("label").isin(0, 1, 2))
    silver = spark.read.parquet(silver_path).select(F.col("id").cast("long").alias("id"), "title", "content")
    df = prepare_text(build_article_text(labels.join(silver, "id")))
    return evaluate(model.transform(df), "manual_200")


def save_metrics(spark, all_metrics, source_name, train_count, test_count):
    rows = []
    for metrics, per_class, _ in all_metrics:
        for k, v in metrics.items():
            if k != "dataset":
                rows.append((metrics["dataset"], k, float(v)))
        for l, c in per_class.items():
            for k, v in c.items():
                rows.append((metrics["dataset"], f"{k}_class_{l}", float(v)))

    # Tạo DataFrame bằng literal (không cần Python worker)
    metrics_path = os.environ.get("MODEL_METRICS_PATH", P.GOLD_MODEL_METRICS)
    lit_rows = [
        F.struct(F.lit(d).alias("dataset"), F.lit(m).alias("metric"), F.lit(v).alias("value"))
        for d, m, v in rows
    ]
    metrics_df = spark.range(1).select(F.explode(F.array(*lit_rows)).alias("r")).select("r.*")
    metrics_df.coalesce(1).write.mode("overwrite").parquet(metrics_path)
    print(f"-> Đã lưu chỉ số đánh giá vào: {metrics_path}")

    # Ghi thêm bản Markdown vào reports/ (khi chạy trong container có mount thư mục dự án)
    if not os.path.isdir(REPORTS_DIR):
        return
    out = os.path.join(REPORTS_DIR, "mllib_metrics_latest.md")
    lines = [
        "# Kết quả đánh giá mô hình cảm xúc (sinh tự động bởi train_sentiment_mllib.py)",
        "",
        f"- Thời điểm chạy: {datetime.now():%Y-%m-%d %H:%M:%S}",
        f"- Dữ liệu huấn luyện: {source_name}",
        f"- Train: {train_count:,} mẫu, Test: {test_count:,} mẫu (khử trùng trước khi chia, không có câu nào nằm ở cả hai tập)",
        "",
    ]
    for metrics, per_class, confusion in all_metrics:
        lines += [f"## {metrics['dataset']}", "", "| Chỉ số | Giá trị |", "|---|---|"]
        lines.append(f"| n_samples | {metrics['n_samples']:,} |")
        for k in ["accuracy", "macro_precision", "macro_recall", "macro_f1", "weighted_f1"]:
            lines.append(f"| {k} | {metrics[k]:.4f} |")
        lines += ["", "| Lớp | Precision | Recall | F1 |", "|---|---|---|---|"]
        for l, c in per_class.items():
            lines.append(f"| {l} {LABEL_NAMES[l]} | {c['precision']:.4f} | {c['recall']:.4f} | {c['f1']:.4f} |")
        lines += ["", "Ma trận nhầm lẫn (hàng = nhãn thật, cột = dự đoán):", "",
                  "| Nhãn | 0 | 1 | 2 |", "|---|---|---|---|"]
        for r in confusion:
            lines.append(f"| {r['label']} | {r['0.0']} | {r['1.0']} | {r['2.0']} |")
        lines.append("")
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"-> Đã ghi báo cáo: {out}")


def main():
    start_time = time.time()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    model_hdfs_path = os.environ.get("MODEL_PATH", P.MODEL_SENTIMENT)

    print("=" * 65)
    print(" BẮT ĐẦU HUẤN LUYỆN SPARK MLLIB: SENTIMENT ANALYSIS (VIETNAMESE)")
    print(f" Đích lưu trữ Model HDFS: {model_hdfs_path}")
    print("=" * 65)

    print("\n[Bước 1/5] Nạp dữ liệu có nhãn và chia Train/Test phân tầng (80/20)...")
    df, source_name = load_training_dataset(spark)
    print(f"-> Nguồn dữ liệu: {source_name}")
    df = prepare_text(df)
    train_df, test_df = stratified_split(df)
    train_count, test_count = train_df.count(), test_df.count()
    print(f"-> Tập huấn luyện (Train): {train_count:,} bản ghi")
    print(f"-> Tập kiểm thử (Test)  : {test_count:,} bản ghi")
    if source_name.startswith("seed_corpus"):
        print("   CẢNH BÁO: đang dùng 45 câu mẫu, tập test rất nhỏ nên chỉ số chỉ mang tính minh họa.")
        print(f"   Đặt tập nhãn thật (CSV text,label) tại {P.LABELS_TRAIN} để có F1 đáng tin cậy.")

    print("\n[Bước 2/5] Đang khớp (fit) Spark MLlib Pipeline...")
    pipeline_model = build_pipeline().fit(add_class_weights(train_df))
    print("-> Huấn luyện mô hình LogisticRegression hoàn tất!")

    print("\n[Bước 3/5] Đánh giá trên tập Test...")
    all_metrics = [evaluate(pipeline_model.transform(test_df), "test_split")]

    print("\n[Bước 4/5] Đánh giá domain shift trên nhãn tay (nếu có)...")
    manual = evaluate_manual_labels(spark, pipeline_model)
    if manual:
        all_metrics.append(manual)
    save_metrics(spark, all_metrics, source_name, train_count, test_count)

    print(f"\n[Bước 5/5] Đang lưu trữ PipelineModel lên HDFS: {model_hdfs_path}...")
    pipeline_model.write().overwrite().save(model_hdfs_path)
    print("-> Đã lưu mô hình thành công lên HDFS!")

    total_time = time.time() - start_time
    print("=" * 65)
    print(f" HOÀN TẤT HUẤN LUYỆN TRONG {total_time:.2f} GIÂY")
    print("=" * 65)

    spark.stop()


if __name__ == "__main__":
    main()
