# Nhật ký thực hiện – BD05 News Lakehouse

Nhật ký ghi các mốc công việc, lỗi gặp phải và cách khắc phục, lịch sử commit và đóng góp của từng thành viên.
Các mốc từ 18/10/2026 trở đi được thực hiện với sự hỗ trợ của Claude Code (trợ lý lập trình AI).

## 1. Các mốc công việc

| Ngày | Công việc | Kết quả |
| --- | --- | --- |
| 01/10/2026 | Khởi tạo repository, cấu hình Docker Compose: HDFS 1 NameNode + 3 DataNode, Spark | Commit `3a80ae0` |
| 08/10/2026 | Pipeline Spark MLlib phân loại cảm xúc, gán nhãn tầng Silver, tổng hợp Gold và Z-score | Commit `f3568a1` |
| 10/10/2026 | Thêm Kafka, Airflow, script producer/consumer | Commit `2fcfa08` |
| 10–14/10/2026 | Gộp nhánh `dev` vào `main` (PR #1–#3) | Commit `1dbf1aa` |
| 18/10/2026 | Đối chiếu code với file phân công, lập danh sách thiếu sót | `docs/kiem_tra_yeu_cau.md` |
| 18–19/10/2026 | Bổ sung chuẩn hoá Unicode NFC, khử trùng SHA-256 + SimHash, FastAPI, sửa rò rỉ dữ liệu khi huấn luyện, DAG gọi `spark-submit` thật; chạy thử bằng Spark local | Code mới, chưa commit |
| 24/10/2026 | Dựng cụm Docker theo compose của repo, nạp 59.168 bài Kaggle lên HDFS, chạy toàn bộ pipeline lần đầu; thêm UIT-VSFC, Delta Lake, LDA, đột biến thực thể, Kafka + Zookeeper, Structured Streaming, 6 DAG, dashboard 6 màn hình | Silver 47.976 bài; Macro-F1 0,689 |
| 25/10/2026 | `lakehouse_master_pipeline` chạy thành công DAG 1 → 5; tối ưu API (p95 ≤ 12 ms) và dashboard; đẩy code lên repo nhóm | Commit `b8e27cd`, `f174b8f` |
| 27/10/2026 | Dựng lại cụm và chạy lại toàn bộ pipeline sau khi image/volume Docker bị xoá; giao diện dashboard dạng website | Commit `d98341c` |
| 28/10/2026 | Gỡ mật khẩu khỏi repo (`.env`), xuất bằng chứng HDFS/Airflow/Kafka/API vào `evidence/`, viết nhật ký | Commit cùng ngày |

## 2. Lỗi gặp phải và cách khắc phục

| # | Lỗi | Nguyên nhân | Cách khắc phục |
| --- | --- | --- | --- |
| 1 | F1 mô hình cảm xúc cao bất thường | 45 câu mẫu bị nhân bản 150 lần rồi mới chia train/test, tập test trùng tập train | Dùng UIT-VSFC (16.174 câu), khử trùng văn bản trước rồi chia phân tầng 80/20, tính thêm Macro-F1 |
| 2 | Không chạy được UDF Spark và `pyspark.ml` trên máy Windows | Python 3.13 chưa được PySpark 3.5 hỗ trợ (thiếu `distutils`, worker lỗi) | Kiểm thử bằng hàm Spark SQL thuần ở local, chạy thật trong container Spark (Python 3.11) |
| 3 | Bước gán nhãn lỗi `No module named 'pandas'` | `pyspark.ml.functions` cần pandas, image Spark không có | Cài `pandas`, `pyarrow` trong `docker/spark/Dockerfile` |
| 4 | Build image Spark lỗi `curl: not found` | Image bitnami không có curl | Tải JAR Delta Lake / Kafka bằng lệnh `ADD` của Dockerfile |
| 5 | Job Spark thứ hai đứng ở trạng thái WAITING rất lâu | Spark standalone cấp toàn bộ core cho job đầu tiên | Tăng worker lên 4 core, đặt `spark.cores.max=2` cho mỗi job |
| 6 | DAG con đứng ở `queued`, DAG điều phối không chạy tiếp | Airflow standalone dùng SequentialExecutor; `TriggerDagRunOperator` chờ DAG con nên chiếm chỗ duy nhất | Bật `deferrable=True` để nhả executor trong lúc chờ |
| 7 | Webserver Airflow tắt: "No response from gunicorn master within 120 seconds" | Máy bận chạy Spark | `AIRFLOW__WEBSERVER__WORKERS=2`, tăng timeout lên 300 giây |
| 8 | DAG điều phối tự chạy ngoài ý muốn sau khi tạo lại container Airflow | Cơ sở dữ liệu SQLite nằm trong container nên bị tạo mới, DAG `@daily` không bị pause | Pause DAG, đánh dấu lần chạy thừa là failed qua REST API |
| 9 | Task Airflow lỗi "Error while fetching server API version … Read timed out" | Docker Desktop treo (API trả 500) | Khởi động lại Docker Desktop, chạy lại DAG lỗi |
| 10 | HDFS trống, mất toàn bộ container (27/10) | Image, container và volume Docker trên máy bị xoá | Dựng lại bằng `docker compose up -d --build`, chạy lại `lakehouse_master_pipeline` |
| 11 | Airflow không mở được sau khi máy tắt đột ngột: "Already running on PID 36" | File PID cũ của webserver còn lại | Xoá file PID; lệnh khởi động container tự xoá file này |
| 12 | Kafka chạy chế độ KRaft, không đúng yêu cầu Kafka + Zookeeper | Image `apache/kafka` mặc định KRaft | Chuyển sang `confluentinc/cp-kafka` + `cp-zookeeper` 7.5.0 |
| 13 | Job LDA lỗi `Column is not iterable` | `F.array_position` không nhận Column làm giá trị | Dùng `F.expr("array_position(dist, topic_prob)")` |
| 14 | SimHash loại nhầm khoảng 520 bài thethaovanhoa.vn | Các bài này chỉ crawl được phần chân trang giống hệt nhau | Giữ nguyên việc loại (bài lỗi crawl), ghi chú rõ trong báo cáo |
| 15 | API `/api/trends?group_by=day_topic` p95 331 ms (> 200 ms) | Trả khoảng 800 KB JSON mỗi lần | Cache kết quả theo tham số, dùng orjson → p95 ≤ 12 ms |
| 16 | Dashboard tải khoảng 4,8 giây | Dựng lại toàn bộ biểu đồ Plotly mỗi lần tải | Cache biểu đồ theo bộ lọc bằng `st.cache_data` → khoảng 0,5 giây |
| 17 | Lệnh `docker exec … hdfs dfs` báo "No FileSystem for scheme C" | Git Bash tự đổi đường dẫn `/lakehouse` thành `C:/…` | Đặt `MSYS_NO_PATHCONV=1` hoặc chạy bằng PowerShell |
| 18 | API gọi Airflow bị timeout | `localhost` thử IPv6 trước | Đổi địa chỉ Airflow thành `127.0.0.1` |
| 19 | Mật khẩu Airflow `admin/admin` nằm trong repo | Ghi trực tiếp trong `docker-compose.yml`, code API và README | Chuyển sang file `.env` (không đưa lên git) + `.env.example` |

## 3. Lịch sử commit

| Commit | Ngày | Tác giả | Nội dung |
| --- | --- | --- | --- |
| `3a80ae0` | 01/10/2026 | NQPDong | Khởi tạo repository, hạ tầng HDFS 3 DataNode và Spark |
| `33206b1` … `664452e` | 01/10/2026 | NQPDong | Dọn tài liệu nháp |
| `f3568a1` | 08/10/2026 | NQPDong | Pipeline Spark MLlib, gán nhãn Silver, Z-score Gold |
| `2fcfa08` | 10/10/2026 | NQPDong | Kafka, Airflow, script streaming |
| `56802a0`, `22862f8`, `1dbf1aa` | 10–14/10/2026 | NQPDong | Gộp PR #1–#3 |
| `b460ced` | 25/10/2026 | Annie | Khởi tạo repo nộp bài |
| `b8e27cd` | 25/10/2026 | Annie | FastAPI, Delta Lake, LDA, đột biến thực thể, streaming, 6 DAG, dashboard |
| `f174b8f` | 25/10/2026 | Annie | Gộp với repo nộp bài |
| `d98341c` | 28/10/2026 | Annie | Dashboard dạng website, kịch bản demo |
| `2ed8840` | 28/10/2026 | Annie | Xoá file kịch bản demo khỏi repo |

Xem đầy đủ bằng `git log --oneline --date=short --format="%h %ad %an %s"`.

## 4. Đóng góp của từng thành viên

Phân vai theo README; nhóm điền họ tên và tỷ lệ đóng góp thực tế.

| Thành viên | Vai trò | Phần việc chính | Đóng góp (%) |
| --- | --- | --- | --- |
| SV1 – [Họ tên] | Lead Data Engineer | Kiến trúc HDFS Lakehouse, job Bronze → Silver, khử trùng lặp | [ ] |
| SV2 – [Họ tên] | Lead ML/DE | Tiền xử lý NLP, mô hình cảm xúc, Z-score, LDA, gán nhãn 200 bài | [ ] |
| SV3 – [Họ tên] | Lead App/Dashboard | FastAPI, dashboard Streamlit, write-back | [ ] |
| SV4 – [Họ tên] | Lead DevOps/Streaming | Docker Compose, Kafka + Spark Streaming, DAG Airflow, script chạy | [ ] |
