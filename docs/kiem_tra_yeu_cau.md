# Kiểm tra code so với yêu cầu trong "Phân Công.docx"

## Cập nhật 25/10/2026: trạng thái sau khi chạy trên cụm thật

Toàn bộ pipeline đã chạy thành công qua Airflow (`lakehouse_master_pipeline` → DAG 1–5, và DAG 6 streaming) trên 59.168 bài.

| Yêu cầu | Trạng thái | Kết quả / file |
|---|---|---|
| HDFS NameNode + 3 DataNode, replication 3 | ✅ | `hdfs fsck` HEALTHY, replication trung bình 3.0 |
| ETL Bronze → Silver, NFC, Exact + SimHash, báo cáo tỷ lệ trùng | ✅ | 47.976 bài Silver; trùng lặp 7,06% (`gold/etl_quality_stats.parquet`) |
| Sentiment MLlib, F1 thật, model trên HDFS | ✅ | UIT-VSFC: Accuracy 0,835, Macro-F1 0,689 (`reports/mllib_metrics_latest.md`) |
| Gán nhãn tay 200 bài (domain shift) | ✅ | Macro-F1 trên báo chí chỉ 0,287 → mô hình train trên phản hồi sinh viên lệch mạnh về "Tiêu cực" khi áp vào tin tức. Nhãn do Claude gán, nhóm cần rà soát (`data/labels/manual_200_sample.csv`) |
| Xu hướng + Z-score | ✅ | `gold/xu_huong_ngay`, `gold/canh_bao_dot_bien` |
| FastAPI 4 endpoint < 200 ms | ✅ | p95 ≤ 12 ms (`scripts/benchmark_api.py`) |
| Dashboard < 2 s, cache | ✅ | 6 màn hình; ~0,4–0,5 s/lần khi đã cache, lần đầu ~5 s |
| Orchestration | ✅ | 6 DAG + 1 DAG điều phối, chạy thật bằng `spark-submit`; `run_all_pipeline.ps1` |
| Docker Compose | ✅ | NameNode, 3 DataNode, Spark (worker 4 core), Kafka + Zookeeper, Airflow |
| Batch ingestion | ✅ | DAG `bd05_1_ingestion` |
| Delta Lake time-travel / OPTIMIZE / VACUUM | ✅ | `silver/bai_viet_delta`, MERGE nhãn write-back, lịch sử ở `gold/delta_lich_su` |
| LDA topic | ✅ | 12 chủ đề (`gold/lda_chu_de`) |
| Đột biến theo thực thể / thương hiệu | ✅ | 313 thực thể (`gold/canh_bao_thuc_the`) |
| Dashboard 6 màn hình, write-back | ✅ | Sửa nhãn → `POST /api/feedback` → Delta MERGE + train lại |
| 6 DAG Airflow, trigger từ web | ✅ | Nút ▶ Chạy trên Dashboard → FastAPI → Airflow REST API |
| Kafka Producer Replay + Spark Structured Streaming | ✅ | 500 bài đã đi qua Kafka → Bronze/Silver streaming |
| Kafka + Zookeeper | ✅ | `confluentinc/cp-kafka` + `cp-zookeeper` |
| Báo cáo 15–25 trang + kịch bản demo | ❌ | Chưa làm (theo yêu cầu) |

Lưu ý: Airflow standalone dùng SequentialExecutor nên mỗi lúc chỉ chạy 1 task; DAG điều phối dùng `deferrable=True` để không bị kẹt.


## Cập nhật 19/10/2026: đã bổ sung

| # | Yêu cầu | Trạng thái mới | File |
|---|---|---|---|
| 2 | ETL: tên Silver, báo cáo tỷ lệ trùng | ✅ | `src/jobs/bronze_to_silver_job.py` ghi `silver/bai_viet_clean.parquet`, `gold/etl_quality_stats.parquet`, `_quarantine/` |
| 3 | `nlp_cleaner.py` (HTML + NFC), tách từ pyvi | ✅ | `src/nlp/nlp_cleaner.py`, `src/nlp/tokenizer.py`, `docker/spark/Dockerfile` (cài pyvi) |
| 4 | `dedup.py` Exact + SimHash 64-bit/Hamming ≤ 3 | ✅ | `src/dedup/dedup.py` (band LSH, chạy thuần Spark SQL) |
| 5 | Sentiment: không rò rỉ, Macro-F1, nhãn tay 200 bài | ✅ code, ⏳ số liệu | `src/models/train_sentiment_mllib.py`, `src/jobs/export_manual_label_sample.py`; cần tập nhãn thật để có F1 đáng tin |
| 7 | FastAPI 4 endpoint | ✅ | `src/api/main.py`, `scripts/run_api.ps1`, `scripts/benchmark_api.py` |
| 9 | Master script có bước train; DAG chạy job thật | ✅ | `scripts/run_all_pipeline.ps1`, `dags/lakehouse_master_dag.py` |
| 11 | Batch ingestion tự động | ✅ | Bước 2 của `run_all_pipeline.ps1` và task 1 của DAG |
| 12 | Báo cáo 15–25 trang + kịch bản demo | ❌ chưa làm (theo yêu cầu) | |

Phần bên dưới là kết quả kiểm tra ban đầu (trước khi bổ sung).

Ngày kiểm tra: 18/10/2026 (commit `1dbf1aa`). Ký hiệu: ✅ Đã làm · 🟡 Làm một phần · ❌ Chưa có.

## Tóm tắt

| # | Yêu cầu bắt buộc | Trạng thái |
|---|---|---|
| 1 | HDFS: NameNode + 3 DataNode, replication ≥ 3, bronze/silver/gold | ✅ |
| 2 | ETL Bronze → Silver (làm sạch, khử trùng, partition theo ngày, báo cáo tỷ lệ trùng) | 🟡 |
| 3 | NLP: `nlp_cleaner.py` (HTML + Unicode NFC), tách từ tiếng Việt | 🟡 |
| 4 | `dedup.py`: Exact Dedup + SimHash 64-bit/Hamming | 🟡 (thiếu SimHash) |
| 5 | Sentiment MLlib Pipeline + F1 thật + lưu model trên HDFS | 🟡 (metric không đáng tin) |
| 6 | Gold: `xu_huong_ngay` + Z-score → `canh_bao_dot_bien` | ✅ |
| 7 | FastAPI: `/api/overview`, `/api/trends`, `/api/sentiment`, `/api/alerts` | ❌ |
| 8 | Dashboard 4 màn (Overview, Trend, Sentiment, Crisis Alert), có cache | ✅ |
| 9 | Orchestration: master script hoặc 1–2 DAG Airflow | 🟡 (DAG chỉ in log) |
| 10 | Docker Compose: NameNode, 2–3 DataNode, Spark cluster | ✅ |
| 11 | Batch ingestion | 🟡 (thủ công) |
| 12 | Báo cáo 15–25 trang (Chương 1–5) + kịch bản demo 15 phút | ❌ |

## Chi tiết từng mục bắt buộc

### 1. HDFS ✅
- `docker-compose.yml`: `namenode` + `datanode`, `datanode2`, `datanode3`, `HDFS_CONF_dfs_replication=3`.
- `docs/huong_dan_cai_dat_hdfs.md` dòng 111–117 tạo `bronze/ silver/ gold/ models/ ...` trong `/lakehouse/bd05_tin_tuc/`.

### 2. ETL Bronze → Silver 🟡
- Có: `src/jobs/bronze_to_silver_job.py` đọc Bronze Parquet, lọc rỗng, bỏ HTML, khử trùng theo `url` rồi theo `(title, source)`, ghi Silver `partitionBy("crawled_date")`.
- Thiếu/chưa khớp:
  - Tên file là `silver/news_clean.parquet`, đề yêu cầu `silver/bai_viet_clean.parquet`.
  - Số bài trùng chỉ được `print` ra console, không ghi thành bảng/file báo cáo. README gộp "rác + trùng" thành 7.547 bài (12,75%), chưa tách riêng tỷ lệ trùng lặp.
  - Không có near-duplicate (SimHash), xem mục 4.

### 3. NLP tiếng Việt 🟡
- Có `src/nlp/text_cleaner.py` (bỏ HTML, khoảng trắng), `src/nlp/tokenizer.py` (pyvi), `src/nlp/stopwords.py`.
- Thiếu:
  - Không có file `nlp_cleaner.py` theo đúng tên.
  - **Không chuẩn hóa Unicode NFC** (không dùng `unicodedata.normalize`).
  - Các module `src/nlp/*` **không được job nào gọi**: ETL tự viết lại regex, còn model MLlib dùng `RegexTokenizer` tách theo khoảng trắng, không dùng pyvi.

### 4. Khử trùng lặp 🟡
- Có `src/dedup/exact_dedup.py` (exact theo url, title+source), nhưng job ETL không import nó mà viết lại `dropDuplicates`.
- **Thiếu hoàn toàn SimHash 64-bit + Hamming distance.** Chỉ được nhắc tới trong `reports/eda_summary.md` và `docs/huong_dan_cai_dat_hdfs.md`, không có code.
- Chưa có file `dedup.py` theo đúng tên.

### 5. Sentiment Spark MLlib 🟡 (vấn đề lớn nhất)
- Đúng: `src/models/train_sentiment_mllib.py` có Pipeline `RegexTokenizer → StopWordsRemover → HashingTF → IDF → LogisticRegression`, lưu model vào `hdfs://namenode:9000/lakehouse/bd05_tin_tuc/models/sentiment_model`. `src/jobs/silver_sentiment_inference.py` nạp model và gán nhãn cho Silver.
- Vấn đề:
  - **Dữ liệu train là 45 câu tự viết nhân bản 150 lần** (`sample_corpus * 150`), sau đó mới `randomSplit`. Tập test chứa đúng những câu đã có trong tập train nên metric bị rò rỉ (data leakage), **không phải "F1 thật"**.
  - Code dùng `metricName="f1"` (weighted F1), **không tính Macro-F1** như đề yêu cầu.
  - `reports/mllib_sentiment_evaluation.md` ghi Accuracy 88,67%, có confusion matrix, và "Macro-F1 0.7820 trên 200 bài gán tay", nhưng **trong repo không có code hay file nhãn nào tạo ra các con số này**. Với dữ liệu nhân bản như trên, chạy thật sẽ ra gần 100%. Nên chạy lại và cập nhật số liệu thật trước khi nộp.
- Cần: dùng dataset có nhãn thật (ví dụ UIT-VSFC hoặc bài gán tay), tách train/test trước khi nhân bản, thêm Macro-F1 (`f1` theo từng lớp rồi lấy trung bình, hoặc `MulticlassMetrics`).

### 6. Xu hướng & đột biến ✅
- `src/jobs/silver_to_gold_job.py`: `gold/xu_huong_ngay.parquet` group theo `crawled_date, topic, source` (có cả tỷ lệ cảm xúc); `gold/canh_bao_dot_bien.parquet` có Z-score số bài và Z-score bài tiêu cực theo chuyên mục, ngưỡng 1,5 và 2,0.
- Lưu ý nhỏ: mean/stddev tính trên toàn bộ chuỗi gồm cả ngày đang xét (không phải rolling window lịch sử). Vẫn chấp nhận được, nhưng nên giải thích trong báo cáo.

### 7. FastAPI ❌
- **Không có code FastAPI nào** trong repo (chỉ được nhắc tới trong comment/print). Không có `/api/overview`, `/api/trends`, `/api/sentiment`, `/api/alerts`, chưa đo được < 200 ms.

### 8. Dashboard ✅
- `src/dashboard/app.py` (Streamlit), `@st.cache_data`, KPI tổng quan + 4 tab: Xu hướng, Cảm xúc, Cảnh báo Z-Score, Kiến trúc.
- Lưu ý: dashboard đọc Gold từ `data/lakehouse/gold/` (bản copy từ HDFS do `run_all_pipeline.ps1` bước 5), không đọc qua API. Chưa có số đo thời gian tải < 2 s.

### 9. Orchestration 🟡
- `scripts/run_all_pipeline.ps1` là master script chạy thật 3 job bằng `spark-submit` → đạt yêu cầu tối thiểu "Master Pipeline script".
  - Nhưng **không chạy bước train model** (`train_sentiment_mllib.py`); nếu HDFS chưa có model thì job 2 lỗi.
- `dags/lakehouse_master_dag.py`: các task **chỉ `print` thông báo cố định** (ví dụ "51,621 bài báo sạch"), không gọi Spark. Nếu demo Airflow thì cần đổi sang `BashOperator`/`SparkSubmitOperator` chạy đúng các job.

### 10. Docker Compose ✅
- NameNode, 3 DataNode, `spark-master` + 1 `spark-worker`, kèm Kafka và Airflow.

### 11. Batch ingestion 🟡
- `scripts/filter_kaggle_data.py` lọc JSON gốc ra Parquet; việc đưa lên HDFS là lệnh tay `hdfs dfs -put` trong `docs/huong_dan_cai_dat_hdfs.md` dòng 141. Nên gom thành 1 script (hoặc bước đầu trong `run_all_pipeline.ps1`).
- Lưu ý: script lọc từ 06/2021–06/2022 nhưng dữ liệu thực tế chỉ có 07/06–30/06/2022 (`reports/eda_summary.md`). Nhận xét về "Covid 07–10/2021" trong EDA không khớp với dữ liệu.

### 12. Báo cáo & demo ❌
- `reports/` chỉ có các ghi chú kỹ thuật dạng `.md`. **Chưa có báo cáo 15–25 trang (Chương 1–5)** và **chưa có kịch bản demo 15 phút**.

## Phần nâng cao (optional)

| Mục | Trạng thái | Ghi chú |
|---|---|---|
| Delta Lake (time-travel, VACUUM, OPTIMIZE) | ❌ | Chỉ dùng Parquet |
| LDA topic | ❌ | |
| Gán nhãn thủ công 200 bài (domain shift) | ❌ | Có viết trong report nhưng không có file nhãn/code |
| Đột biến theo thực thể/thương hiệu | ❌ | Chỉ theo chuyên mục |
| Dashboard 6 màn, write-back | ❌ | Có 4 tab |
| 6 DAG Airflow, trigger từ web | ❌ | 1 DAG giả lập |
| Kafka Producer Replay + Spark Structured Streaming | 🟡 | Có `src/streaming/kafka_producer_news.py` (replay); consumer dùng `kafka-console-consumer` + pandas, **không phải Spark Structured Streaming** |
| Kafka + Zookeeper | 🟡 | Có Kafka (chế độ KRaft, không cần Zookeeper) |

## Ưu tiên cần làm

1. Viết FastAPI với 4 endpoint đọc Gold Parquet (thiếu hoàn toàn, là yêu cầu bắt buộc).
2. Sửa phần sentiment: dữ liệu có nhãn thật, tách train/test đúng, tính Macro-F1, cập nhật lại số liệu trong report.
3. Thêm SimHash 64-bit + Hamming vào dedup, thêm chuẩn hóa Unicode NFC, và cho job ETL dùng các module `src/nlp`, `src/dedup`.
4. Ghi tỷ lệ trùng lặp ra file/bảng; đổi tên Silver thành `bai_viet_clean.parquet` cho khớp đề.
5. Thêm bước train model vào `run_all_pipeline.ps1`; cho DAG Airflow gọi job thật.
6. Viết báo cáo 15–25 trang và kịch bản demo 15 phút.
