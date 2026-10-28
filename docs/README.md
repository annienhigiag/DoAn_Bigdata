# BD05 News Lakehouse – Công nghệ và hướng dẫn cài đặt

Hệ thống Lakehouse theo dõi xu hướng và cảm xúc tin tức tiếng Việt. Dữ liệu 59.168 bài báo (06/2022) đi qua
HDFS Bronze → Silver → Gold, được xử lý bằng Apache Spark, điều phối bằng Apache Airflow, phục vụ qua FastAPI và
Dashboard Streamlit; luồng tin mới đi qua Kafka → Spark Structured Streaming.

---

## 1. Kiến trúc tổng quan

```text
Kaggle JSON ──filter──> Parquet ──(DAG 1)──> HDFS Bronze (3 DataNodes, replication 3)
Kafka Producer Replay ──> Kafka (+ Zookeeper) ──(DAG 6: Spark Structured Streaming)──> Bronze/Silver streaming
Bronze ──(DAG 2: PySpark ETL – HTML, Unicode NFC, SHA-256 + SimHash)──> Silver bai_viet_clean (phân vùng theo ngày)
UIT-VSFC + nhãn write-back ──(DAG 3: Spark MLlib TF-IDF + LogisticRegression)──> models/sentiment_model
Silver ──(DAG 4: scoring + Spark SQL + Z-score)──> Gold xu_huong_ngay, canh_bao_dot_bien
Silver ──(DAG 5: Delta Lake MERGE/OPTIMIZE/VACUUM + thực thể + LDA)──> Gold nâng cao
Gold ──đồng bộ về máy──> FastAPI (cache, < 200 ms) + Dashboard Streamlit (6 màn hình, write-back)
```

---

## 2. Công nghệ sử dụng

### 2.1. Hạ tầng (Docker Compose)

| Dịch vụ | Image / phiên bản | Container | Cổng | Vai trò |
| --- | --- | --- | --- | --- |
| HDFS NameNode | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | `namenode` | 9870 (UI), 9000 (RPC) | Quản lý metadata hệ thống file phân tán |
| HDFS DataNode ×3 | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | `datanode`, `datanode2`, `datanode3` | 9864–9866 | Lưu block dữ liệu, `dfs.replication = 3` |
| Spark Master | `bd05/spark:3.5.1-pyvi` (build từ `docker/spark/Dockerfile`) | `spark-master` | 8080 (UI), 7077 | Điều phối job Spark standalone |
| Spark Worker | `bd05/spark:3.5.1-pyvi` | `spark-worker` | 8081 | 4 core, 3 GB RAM; mỗi job tối đa 2 core |
| Zookeeper | `confluentinc/cp-zookeeper:7.5.0` | `zookeeper` | 2181 | Quản lý cụm Kafka |
| Kafka | `confluentinc/cp-kafka:7.5.0` | `kafka` | 9094 (từ máy host), 9092 (trong mạng Docker) | Hàng đợi sự kiện tin tức, topic `news-topic` |
| Airflow | `apache/airflow:2.9.1` (standalone) | `airflow` | 8085 | Điều phối 6 DAG + 1 DAG tổng |

Image Spark tự build từ `bitnamilegacy/spark:3.5.1`, cài thêm:

| Thành phần | Phiên bản | Dùng cho |
| --- | --- | --- |
| pyvi | 0.1.1 | Tách từ tiếng Việt |
| pandas, pyarrow | < 2.3, < 18 | `pyspark.ml.functions`, chuyển đổi Arrow |
| delta-spark (Python + JAR) | 3.2.0 | Delta Lake: MERGE, time-travel, OPTIMIZE, VACUUM |
| spark-sql-kafka-0-10, kafka-clients, commons-pool2 | 3.5.1, 3.4.1, 2.11.1 | Spark Structured Streaming đọc Kafka |

### 2.2. Xử lý dữ liệu và học máy

| Công nghệ | Dùng ở đâu | Chi tiết |
| --- | --- | --- |
| Apache Spark 3.5.1 (PySpark) | `src/jobs/`, `src/models/`, `src/streaming/` | ETL, Spark SQL, Window function, MLlib, Structured Streaming |
| Parquet (nén Snappy) | Bronze, Silver, Gold trên HDFS | Silver/Gold phân vùng theo `crawled_date` |
| Delta Lake 3.2.0 | `silver/bai_viet_delta` | MERGE nhãn write-back, time-travel, OPTIMIZE ZORDER BY source, VACUUM |
| Unicode NFC (`unicodedata`) | `src/nlp/nlp_cleaner.py` | Chuẩn hóa dấu tiếng Việt trước khi khử trùng và trích đặc trưng |
| SHA-256 + SimHash 64-bit | `src/dedup/dedup.py` | Khử trùng chính xác và gần trùng (Hamming ≤ 3, band LSH 4 × 16 bit, chạy thuần Spark SQL) |
| Spark MLlib | `src/models/train_sentiment_mllib.py` | pyvi → RegexTokenizer → StopWordsRemover → HashingTF → IDF → LogisticRegression (trọng số lớp) |
| UIT-VSFC | `scripts/prepare_sentiment_labels.py` | 16.174 câu gán nhãn tay (0 tiêu cực, 1 trung tính, 2 tích cực) để huấn luyện |
| Z-score | `src/jobs/silver_to_gold_job.py`, `src/jobs/entity_trend_job.py` | Phát hiện đột biến theo chuyên mục và theo thực thể/thương hiệu |
| LDA (Spark MLlib) | `src/jobs/lda_topic_job.py` | 12 chủ đề ẩn, CountVectorizer + LDA online |

### 2.3. Ứng dụng

| Công nghệ | Phiên bản | Dùng ở đâu |
| --- | --- | --- |
| FastAPI + Uvicorn | ≥ 0.110, ≥ 0.29 | `src/api/main.py` – API đọc Gold Parquet, cache 2 lớp, orjson |
| Streamlit + Plotly | ≥ 1.50, ≥ 5.20 | `src/dashboard/app.py` – Dashboard 6 màn hình, giao diện ở `src/dashboard/theme.py` |
| kafka-python | ≥ 2.0.2 | `src/streaming/kafka_producer_news.py` – Producer phát lại tin vào Kafka |
| Docker SDK for Python | có sẵn trong image Airflow | `dags/lakehouse_common.py` – task Airflow gọi `spark-submit` trong container Spark |

---

## 3. Yêu cầu hệ thống

| Thành phần | Yêu cầu |
| --- | --- |
| Hệ điều hành | Windows 10/11 64-bit (đã kiểm thử trên Windows 11), bật WSL 2 |
| Docker Desktop | Bản mới, WSL 2 backend; cấp ít nhất 6 GB RAM, 4 CPU |
| Dung lượng trống | Khoảng 20 GB (image Docker ~12 GB, dữ liệu + volume HDFS ~2 GB) |
| Python | 3.10 trở lên trên máy host (cho API, Dashboard, script) |
| Dữ liệu | `news_dataset.json` (~655 MB) của [Vietnamese Online News Dataset](https://www.kaggle.com/datasets/haitranquangofficial/vietnamese-online-news-dataset) – cần tài khoản Kaggle để tải |

---

## 4. Cài đặt chi tiết

Mọi lệnh chạy trong **PowerShell**, tại thư mục gốc dự án `vietnamese-news-trend-and-sentiment`.

### Bước 1. Lấy mã nguồn

```powershell
git clone https://github.com/NQPDong/vietnamese-news-trend-and-sentiment.git
cd vietnamese-news-trend-and-sentiment
```

### Bước 2. Đặt dữ liệu gốc

Tải dataset từ Kaggle, giải nén và đặt file vào:

```text
data/raw_kaggle/news_dataset.json
```

Tạo file `.env` chứa tài khoản Airflow (file này không được đưa lên git):

```powershell
Copy-Item .env.example .env   # rồi sửa AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD
```

### Bước 3. Tạo môi trường Python trên máy host

```powershell
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
```

### Bước 4. Chuẩn bị dữ liệu và nhãn

```powershell
# Lọc JSON Kaggle -> data/filtered_2021_2022/news_2021_2022.parquet (59.168 bài)
.venv\Scripts\python scripts/filter_kaggle_data.py

# Tải UIT-VSFC từ Hugging Face -> data/labels/sentiment_train.csv
.venv\Scripts\python scripts/prepare_sentiment_labels.py
```

Tùy chọn: đặt `data/labels/manual_200.csv` (cột `id,label`, 200 bài báo gán nhãn tay) để đo domain shift. Thư mục `data/` không đưa lên git, nên file này cần copy riêng giữa các máy.

### Bước 5. Khởi động cụm Docker

```powershell
docker compose up -d --build
```

Lần đầu mất vài phút để build image Spark (cài pyvi, Delta Lake, Kafka connector). Kiểm tra:

```powershell
docker ps                                               # 9 container: namenode, datanode×3, spark-master, spark-worker, zookeeper, kafka, airflow
docker exec namenode hdfs dfsadmin -report | Select-String "Live datanodes"   # phải là 3
```

| Kiểm tra | Địa chỉ | Kết quả mong đợi |
| --- | --- | --- |
| HDFS | http://localhost:9870 | Tab Datanodes có 3 node In Service |
| Spark | http://localhost:8080 | 1 worker, 4 cores |
| Airflow | http://localhost:8085 (tài khoản trong `.env`) | Thấy 7 DAG `bd05_*` và `lakehouse_master_pipeline` |

### Bước 6. Chạy pipeline dữ liệu (khoảng 1 giờ)

**Cách 1 – Airflow:** mở http://localhost:8085, bật và chạy DAG `lakehouse_master_pipeline`. DAG này chạy lần lượt:

| DAG | Nội dung |
| --- | --- |
| `bd05_1_ingestion` | Tạo thư mục Lakehouse, nạp Bronze (replication 3) và các tập nhãn lên HDFS |
| `bd05_2_etl_clean` | Bronze → Silver: làm sạch HTML, Unicode NFC, khử trùng SHA-256 + SimHash |
| `bd05_3_ml_train` | Huấn luyện và đánh giá mô hình cảm xúc, lưu model lên HDFS |
| `bd05_4_scoring_gold` | Gán nhãn cảm xúc, Spark SQL, bảng Gold xu hướng + Z-score, đồng bộ Gold về máy |
| `bd05_5_advanced_analytics` | Delta Lake, đột biến theo thực thể, chủ đề LDA, đồng bộ Gold |

`bd05_6_streaming` chạy riêng (15 phút một lần hoặc bấm chạy tay).

**Cách 2 – script một lệnh:**

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_all_pipeline.ps1
# Tùy chọn: -SkipTrain (dùng lại model đã có), -SkipAdvanced (bỏ Delta Lake, thực thể, LDA)
```

Sau khi chạy xong, thư mục `data/lakehouse/gold/` trên máy có các bảng Gold cho API và Dashboard.

### Bước 7. Bật API và Dashboard

Mỗi lệnh một cửa sổ PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_api.ps1         # http://localhost:8000/docs
powershell -ExecutionPolicy Bypass -File scripts/run_dashboard.ps1   # http://localhost:8501
```

### Bước 8. Kiểm tra

```powershell
# Độ trễ API (yêu cầu p95 < 200 ms)
.venv\Scripts\python scripts/benchmark_api.py --base http://127.0.0.1:8000

# Phát lại 200 bài vào Kafka, sau đó chạy DAG bd05_6_streaming trong Airflow
.venv\Scripts\python -m src.streaming.kafka_producer_news --limit 200
```

---

## 5. Cấu trúc thư mục

```text
vietnamese-news-trend-and-sentiment/
├── docker-compose.yml            # Cụm HDFS, Spark, Kafka + Zookeeper, Airflow
├── docker/spark/Dockerfile       # Image Spark 3.5.1 + pyvi + Delta Lake + Kafka connector
├── requirements.txt              # Thư viện Python trên máy host
├── dags/                         # 6 DAG chức năng + DAG điều phối + hàm dùng chung
├── scripts/                      # Lọc dữ liệu, tải nhãn, chạy pipeline/API/Dashboard, benchmark
├── src/
│   ├── nlp/                      # nlp_cleaner (HTML, NFC), tách từ pyvi, stopwords
│   ├── dedup/                    # Exact (SHA-256) + SimHash 64-bit
│   ├── jobs/                     # ETL, scoring, Spark SQL, Gold, thực thể, LDA, Delta Lake
│   ├── models/                   # Huấn luyện & đánh giá mô hình cảm xúc
│   ├── streaming/                # Kafka producer, Spark Structured Streaming
│   ├── api/                      # FastAPI
│   ├── dashboard/                # Streamlit (app.py, theme.py)
│   └── utils/                    # Đường dẫn Lakehouse, SparkSession
├── data/                         # Dữ liệu (không đưa lên git)
└── reports/                      # Báo cáo kỹ thuật, kết quả đánh giá mô hình
```

Cấu trúc Lakehouse trên HDFS (`hdfs:///lakehouse/bd05_tin_tuc/`):

| Thư mục | Nội dung |
| --- | --- |
| `bronze/` | Dữ liệu thô `news_2021_2022.parquet`, `streaming/news` |
| `silver/` | `bai_viet_clean.parquet`, `news_sentiment.parquet`, `bai_viet_delta` (Delta), `streaming_sentiment` |
| `gold/` | `xu_huong_ngay`, `canh_bao_dot_bien`, `canh_bao_thuc_the`, `lda_chu_de`, `etl_quality_stats`, … |
| `models/` | `sentiment_model`, `lda_model` |
| `labels/` | `sentiment_train.csv`, `manual_200.csv`, `feedback.csv` (write-back) |
| `checkpoints/` | Checkpoint của Spark Structured Streaming |
| `_quarantine/` | Bản ghi lỗi bị loại khỏi Silver |

---

## 6. Xử lý sự cố khi cài đặt

| Hiện tượng | Cách xử lý |
| --- | --- |
| `docker compose up` báo trùng tên container | Một container cũ cùng tên (`namenode`, `spark-master`, …) đang tồn tại: `docker rm -f <tên>` hoặc đổi tên |
| HDFS chỉ có dưới 3 DataNode | Đợi 1–2 phút rồi kiểm tra lại; vẫn thiếu thì `docker compose restart datanode datanode2 datanode3` |
| Airflow không vào được http://localhost:8085 | Webserver khởi động chậm khi máy bận: đợi vài phút hoặc `docker compose restart airflow` |
| DAG đứng ở trạng thái queued | Airflow standalone chỉ chạy 1 task mỗi lúc; đợi task trước xong |
| Task Airflow báo lỗi Docker API | Docker Desktop đang treo: khởi động lại Docker Desktop rồi `docker compose up -d` |
| Dashboard báo thiếu dữ liệu Gold | Chạy lại DAG `bd05_4_scoring_gold` (có bước đồng bộ Gold về máy) |
| `run_api.ps1` / `run_dashboard.ps1` không chạy | Chạy PowerShell với `-ExecutionPolicy Bypass` như hướng dẫn, hoặc cài lại `.venv` |
