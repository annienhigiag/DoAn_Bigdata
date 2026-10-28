# HỆ THỐNG LAKEHOUSE THEO DÕI XU HƯỚNG VÀ CẢM XÚC TIN TỨC TIẾNG VIỆT

> **Môn học:** Dữ liệu lớn (Big Data)  
> **Kiến trúc:** Lakehouse Medallion (HDFS 3 DataNodes • Apache Spark 3.5 MLlib • Parquet + Delta Lake • Kafka/Zookeeper • Airflow • FastAPI • Streamlit Dashboard)  
> **Dữ liệu thực nghiệm:** Vietnamese Online News Dataset (Tháng 06/2022 - 59.168 bài báo thô ➔ 51.621 bài báo sạch)  

---

## 1. TỔNG QUAN KIẾN TRÚC HỆ THỐNG

Hệ thống được thiết kế theo mô hình **Lakehouse Medallion (Bronze ➔ Silver ➔ Gold)** kết hợp cụm tính toán phân tán **Apache Spark** và cụm lưu trữ phân tán **Hadoop HDFS**:

```text
                     ┌───────────────────────────┐
                     │ Nguồn dữ liệu báo điện tử │ (59.168 bài báo thô)
                     └─────────────┬─────────────┘
                                   │
                                   ▼ (Ingestion / Replication = 3)
┌─────────────────────────────────────────────────────────────────────────────┐
│                              HADOOP HDFS CLUSTER                            │
│                  (1 NameNode: 9000/9870  •  3 DataNodes: 9864-9866)         │
│                                                                             │
│  ┌────────────────────┐   PySpark Data Quality   ┌────────────────────────┐ │
│  │    TẦNG BRONZE     │ ───────────────────────> │      TẦNG SILVER       │ │
│  │ (Dữ liệu thô Parquet)│  Loại HTML & Dedup     │ (51.621 bài sạch, 0 Null│ │
│  └────────────────────┘                          │  24 phân vùng theo ngày)│ │
│                                                  └───────────┬────────────┘ │
│                                                              │              │
│  ┌────────────────────┐   Aggregation & Z-score  ┌───────────▼────────────┐ │
│  │     TẦNG GOLD      │ <─────────────────────── │  SPARK MLLIB PIPELINE  │ │
│  │ - xu_huong_ngay    │    Window Functions      │ - RegexTokenizer       │ │
│  │ - canh_bao_dot_bien│                          │ - StopWordsRemover     │ │
│  └─────────┬──────────┘                          │ - HashingTF + IDF      │ │
│            │                                     │ - LogisticRegression   │ │
│            │                                     └────────────────────────┘ │
└────────────┼────────────────────────────────────────────────────────────────┘
             │
             ▼
┌───────────────────────────┐
│     STREAMLIT GUI         │ ➔ Giao diện trực quan hóa thời gian thực
│  (http://localhost:8501)  │ ➔ Biểu đồ xu hướng, cơ cấu cảm xúc, cảnh báo Z-score
└───────────────────────────┘
```

---

## 2. CẤU TRÚC THƯ MỤC DỰ ÁN

Dự án được chuẩn hóa phân lớp rõ ràng, loại bỏ toàn bộ các file rác và trùng lặp:

```text
D:\BigData\
├── docker-compose.yml              # Khởi tạo cụm phân tán: NameNode, 3 DataNodes, Spark Master/Worker, Kafka, Airflow
├── docker/spark/Dockerfile         # Image Spark 3.5.1 có cài pyvi
├── dags/lakehouse_master_dag.py    # DAG Airflow chạy thật các Spark job
├── requirements.txt                # Thư viện cho API/Dashboard trên máy host
├── README.md                       # Tài liệu hướng dẫn tổng quan và vận hành đồ án
├── docs/                           # Tài liệu đề cương, ghi chú yêu cầu của thầy và phân công
│   ├── phan_cong.docx              # Phân công trách nhiệm 4 thành viên
│   ├── huong_dan_cai_dat_hdfs.md   # Hướng dẫn chi tiết triển khai cụm HDFS
│   └── notes.txt                   # Ghi chú 
├── reports/                        # Báo cáo thực nghiệm kỹ thuật và đo lường
│   ├── eda_summary.md              # Khám phá dữ liệu 59.168 bài báo
│   ├── benchmark_udf.md            # Benchmark Pandas UDF vs Standard UDF
│   ├── mllib_sentiment_evaluation.md # Đánh giá mô hình học máy phân tích cảm xúc
│   └── gold_trend_and_anomaly_analysis.md # Phân tích xu hướng và cảnh báo đột biến Z-Score
├── src/                            # Mã nguồn ứng dụng (Python packages chuẩn)
│   ├── nlp/                        # nlp_cleaner.py (HTML, Unicode NFC), tách từ pyvi, stopwords
│   ├── dedup/                      # dedup.py: Exact Dedup (URL, SHA-256) + SimHash 64-bit/Hamming
│   ├── models/                     # Huấn luyện + đánh giá mô hình Spark MLlib (Macro-F1)
│   ├── api/                        # FastAPI đọc Gold Parquet: /api/overview, trends, sentiment, alerts
│   ├── jobs/                       # Toàn bộ các Spark Jobs ETL chuyển dịch Lakehouse
│   │   ├── bronze_to_silver_job.py # Job 1: Làm sạch và khử trùng lặp dữ liệu
│   │   ├── silver_sentiment_inference.py # Job 2: Gán nhãn cảm xúc bằng MLlib
│   │   ├── export_manual_label_sample.py # Xuất 200 bài để gán nhãn tay (domain shift)
│   │   └── silver_to_gold_job.py   # Job 3: Tổng hợp bảng Gold và tính điểm Z-score
│   ├── dashboard/                  # Giao diện trực quan hóa dữ liệu (GUI Streamlit)
│   │   └── app.py                  # Mã nguồn ứng dụng Dashboard
│   └── utils/                      # Tiện ích tạo SparkSession tối ưu
├── scripts/                        # Các kịch bản chạy tự động 1-Click
│   ├── run_all_pipeline.ps1        # Kịch bản chạy toàn bộ pipeline End-to-End
│   ├── run_dashboard.ps1           # Kịch bản khởi động Dashboard
│   ├── run_api.ps1                 # Kịch bản khởi động FastAPI (cổng 8000)
│   ├── benchmark_api.py            # Đo độ trễ API (yêu cầu < 200 ms)
│   ├── simulate_news_stream.py     # Script giả lập sinh luồng tin tức mới
│   └── filter_kaggle_data.py       # Script trích xuất dữ liệu gốc ban đầu
└── data/                           # Lưu trữ dữ liệu tĩnh (được .gitignore bảo vệ)
```

---

## 3. KẾT QUẢ ĐO LƯỜNG VÀ NGHIỆM THU THỰC TẾ

| Hạng mục | Số liệu thực nghiệm | Ghi chú kỹ thuật |
| :--- | :--- | :--- |
| **Dung lượng lưu trữ HDFS** | 3 DataNodes, `replication = 3` | Trạng thái `HEALTHY`, dung lượng trống ~2.78 TB |
| **Dữ liệu thô (Bronze)** | **59.168** bài báo | File `news_2021_2022.parquet` (97.4 MB, nén Snappy) |
| **Dữ liệu sạch (Silver)** | **47.976** bài báo (`silver/bai_viet_clean.parquet`) | Loại 7.548 bài rỗng/quá ngắn (12,76%) và 3.644 bài trùng lặp (7,06%: 549 trùng nội dung SHA-256, 3.095 near-duplicate SimHash) — chạy ngày 24/10/2026, chi tiết ở `gold/etl_quality_stats.parquet` |
| **API FastAPI** | p95 ≤ 12 ms cho cả 5 endpoint chính | `scripts/benchmark_api.py`, 50 request/endpoint, dữ liệu Gold thật |
| **Dashboard** | 6 màn hình; ~0,4–0,5 s/lần chạy script khi đã cache, ~5 s lần đầu (nạp Parquet) | Đo bằng `streamlit.testing.AppTest` |
| **Phân vùng dữ liệu (Partitions)** | **24 ngày** (07/06 - 30/06/2022) | Tối ưu hóa truy vấn Partition Pruning |
| **Mô hình Spark MLlib** | Accuracy 0,835 • Macro-F1 0,689 (test UIT-VSFC) • Macro-F1 0,287 trên 200 bài báo gán tay | pyvi ➔ TF-IDF ➔ LogisticRegression có trọng số lớp; độ lệch miền lớn, chi tiết ở `reports/mllib_metrics_latest.md` |
| **Phân tích nâng cao** | 12 chủ đề LDA • 313 thực thể theo dõi đột biến • Delta Lake 9 phiên bản • 500 bài qua Kafka streaming | DAG `bd05_5_advanced_analytics`, `bd05_6_streaming` |
| **Phát hiện đột biến ($Z$-score)** | Phân vị $|Z| > 1.5$ và $|Z| > 2.0$ | Cảnh báo các sự kiện bùng nổ truyền thông theo chủ đề |

---

## 4. HƯỚNG DẪN KHỞI CHẠY VÀ DEMO (QUICK START)

### Bước 1: Khởi động cụm HDFS và Spark phân tán
Mở Windows PowerShell tại thư mục dự án và chạy (lần đầu cần `--build` để tạo image Spark có pyvi):
```powershell
docker compose up -d --build
```
*Kiểm tra Web UI:*
- **HDFS NameNode UI:** [http://localhost:9870](http://localhost:9870)
- **Spark Master UI:** [http://localhost:8080](http://localhost:8080)

### Bước 2: Chạy toàn bộ Data Pipeline

**Cách 1 – Airflow (khuyến nghị khi demo):** mở [http://localhost:8085](http://localhost:8085) (tài khoản đặt trong file `.env`). Có 6 DAG chức năng và 1 DAG điều phối, mỗi task gọi `spark-submit` thật trong container `spark-master`:

| DAG | Nội dung |
| :--- | :--- |
| `bd05_1_ingestion` | Batch ingestion: nạp Bronze (replication 3) và các tập nhãn lên HDFS |
| `bd05_2_etl_clean` | ETL Clean: HTML, Unicode NFC, Exact + SimHash dedup → Silver |
| `bd05_3_ml_train` | Huấn luyện TF-IDF + LogisticRegression (UIT-VSFC + nhãn write-back), đo Macro-F1 |
| `bd05_4_scoring_gold` | Gán nhãn cảm xúc Silver → Spark SQL → Gold xu hướng + Z-score → đồng bộ Gold |
| `bd05_5_advanced_analytics` | Delta Lake (MERGE write-back, time-travel, OPTIMIZE, VACUUM) → đột biến thực thể → LDA |
| `bd05_6_streaming` | Kafka → Spark Structured Streaming → Bronze/Silver streaming (15 phút/lần) |
| `lakehouse_master_pipeline` | Chạy lần lượt DAG 1 → 5 (TriggerDagRunOperator, chờ từng DAG xong) |

Mọi DAG cũng chạy được từ Dashboard (tab **Pipeline & Write-back**, nút ▶ Chạy → FastAPI → Airflow REST API).

**Cách 2 – script 1-click:**
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_all_pipeline.ps1   # thêm -SkipTrain / -SkipAdvanced nếu cần
```

**Nhãn cảm xúc:**
- `python scripts/prepare_sentiment_labels.py` tải UIT-VSFC (~16.000 câu gán nhãn tay, 0 = Tiêu cực, 1 = Trung tính, 2 = Tích cực) → `data/labels/sentiment_train.csv`; DAG 1 đưa lên HDFS `labels/`.
- `data/labels/manual_200.csv`: 200 bài báo gán nhãn tay để đo domain shift (`manual_200_sample.csv` có tiêu đề + sapo để rà soát).
- Nhãn người dùng sửa trên Dashboard được lưu ở `data/writeback/feedback.csv`, MERGE vào bảng Delta và cộng vào tập huấn luyện ở lần train sau.

### Bước 2b: Mở FastAPI
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_api.ps1
python scripts/benchmark_api.py   # đo p50/p95, yêu cầu < 200 ms
```
Endpoints chính: `/api/overview`, `/api/trends`, `/api/sentiment`, `/api/alerts` (lọc `start`, `end`, `source`, `topic`).
Nâng cao: `/api/topics` (LDA), `/api/entities` (đột biến thực thể), `/api/articles`, `/api/lakehouse/delta-history`, `/api/streaming/stats`,
`POST /api/feedback` (write-back), `GET /api/pipeline/dags`, `POST /api/pipeline/{dag_id}/trigger`, `POST /api/streaming/replay`.
Swagger: [http://localhost:8000/docs](http://localhost:8000/docs).

### Bước 3: Mở Giao diện Dashboard (6 màn hình)
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_dashboard.ps1
```
Truy cập **[http://localhost:8501](http://localhost:8501)**: Tổng quan & Xu hướng • Cảm xúc • Cảnh báo đột biến • Chủ đề LDA • Thực thể & Thương hiệu • Pipeline & Write-back.

### Bước 4: Streaming (Kafka Producer Replay → Spark Structured Streaming)
```powershell
.venv\Scripts\python -m src.streaming.kafka_producer_news --limit 500 --rate 50   # phát lại tin vào Kafka
docker exec -e STREAM_MODE=continuous spark-master spark-submit --master spark://spark-master:7077 `
    --conf spark.cores.max=2 --conf spark.executorEnv.PYTHONPATH=/opt/spark-apps `
    /opt/spark-apps/src/streaming/spark_streaming_job.py   # chạy liên tục khi demo
```
Hoặc bấm **📡 Phát lại vào Kafka** trên Dashboard rồi chạy DAG `bd05_6_streaming`. Kafka chạy cùng Zookeeper (`confluentinc/cp-kafka` + `cp-zookeeper`).

---

## 5. PHÂN CÔNG VAI TRÒ 4 THÀNH VIÊN

- **SV1 (Lead Data Engineer):** Thiết kế kiến trúc HDFS Lakehouse, phân vùng Parquet, xây dựng job `bronze_to_silver_job.py`.
- **SV2 (Lead ML/DE):** Tiền xử lý NLP tiếng Việt, huấn luyện mô hình phân loại cảm xúc `train_sentiment_mllib.py`, xây dựng thuật toán phát hiện đột biến $Z$-score.
- **SV3 (Lead App/Dashboard):** Thiết kế và lập trình giao diện trực quan hóa Streamlit (`src/dashboard/app.py`), tối ưu hóa hiển thị dữ liệu tầng Gold.
- **SV4 (Lead DevOps/Streaming):** Cấu hình Docker Compose (HDFS 3 DataNodes + Spark Cluster), viết script điều phối tự động `run_all_pipeline.ps1` và script giả lập luồng `simulate_news_stream.py`.
