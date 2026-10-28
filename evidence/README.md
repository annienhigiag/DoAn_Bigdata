# Bằng chứng chạy hệ thống

Xuất ngày 28/10/2026 từ cụm Docker đang chạy (HDFS 3 DataNode, Spark 3.5.1, Kafka + Zookeeper, Airflow 2.9.1).

| Thư mục / file | Nội dung | Yêu cầu hồ sơ |
| --- | --- | --- |
| `hdfs/01_dfsadmin_report.txt` | `hdfs dfsadmin -report`: 3 DataNode live, dung lượng | Lệnh và kết quả kiểm tra HDFS |
| `hdfs/02_fsck.txt` | `hdfs fsck /lakehouse/bd05_tin_tuc`: HEALTHY, replication trung bình 3.0 | Lệnh và kết quả kiểm tra HDFS |
| `hdfs/03_ls_lakehouse.txt` | `hdfs dfs -ls -R`: cấu trúc bronze / silver / gold / models / labels / checkpoints / _quarantine | Lệnh và kết quả kiểm tra HDFS |
| `hdfs/04_du_replication.txt` | Dung lượng từng tầng, replication của file Bronze | Lệnh và kết quả kiểm tra HDFS |
| `airflow/01_dag_runs.md` | Lịch sử các lần chạy của 7 DAG | DAG và log Airflow |
| `airflow/logs/<dag>/<task>__attempt<n>` | Log từng task của lần chạy thành công gần nhất. Các task Spark chứa nguyên output `spark-submit` (số bản ghi, báo cáo trùng lặp, metric mô hình, kết quả Spark SQL) | DAG và log Airflow; mã Spark, truy vấn, output/log |
| `kafka/01_topic_offsets.txt` | Topic `news-topic` (3 partition), offset từng partition | Streaming: cấu hình/topic |
| `kafka/02_producer.log` | Log Kafka Producer Replay | Streaming: producer |
| `kafka/03_broker_log_excerpt.txt` | Trích log broker Kafka | Streaming: log Kafka |
| `pipeline_output/api_*.json` | Đầu ra Gold do FastAPI trả về: tổng quan, cảnh báo, thực thể, chủ đề LDA, streaming, lịch sử Delta | Bằng chứng giao diện dùng đầu ra pipeline |

Mã nguồn tương ứng: Spark jobs ở `src/jobs/`, mô hình ở `src/models/`, streaming ở `src/streaming/`
(producer `kafka_producer_news.py`, consumer `spark_streaming_job.py`), DAG ở `dags/`, cấu hình Kafka/Zookeeper ở `docker-compose.yml`.
Kết quả đánh giá mô hình: `reports/mllib_metrics_latest.md`.
