# Kết quả đánh giá mô hình cảm xúc (sinh tự động bởi train_sentiment_mllib.py)

- Thời điểm chạy: 2026-10-25 11:10:51
- Dữ liệu huấn luyện: hdfs://namenode:9000/lakehouse/bd05_tin_tuc/labels/sentiment_train.csv + 2 nhãn sửa tay từ Dashboard
- Train: 12,911 mẫu, Test: 3,265 mẫu (khử trùng trước khi chia, không có câu nào nằm ở cả hai tập)

## test_split

| Chỉ số | Giá trị |
|---|---|
| n_samples | 3,265 |
| accuracy | 0.8349 |
| macro_precision | 0.6779 |
| macro_recall | 0.7437 |
| macro_f1 | 0.6893 |
| weighted_f1 | 0.8516 |

| Lớp | Precision | Recall | F1 |
|---|---|---|---|
| 0 Tiêu cực | 0.8784 | 0.8379 | 0.8576 |
| 1 Trung tính | 0.2262 | 0.5352 | 0.3180 |
| 2 Tích cực | 0.9291 | 0.8581 | 0.8922 |

Ma trận nhầm lẫn (hàng = nhãn thật, cột = dự đoán):

| Nhãn | 0 | 1 | 2 |
|---|---|---|---|
| 0 | 1235 | 154 | 85 |
| 1 | 43 | 76 | 23 |
| 2 | 128 | 106 | 1415 |

## manual_200

| Chỉ số | Giá trị |
|---|---|
| n_samples | 194 |
| accuracy | 0.3505 |
| macro_precision | 0.3261 |
| macro_recall | 0.3684 |
| macro_f1 | 0.2867 |
| weighted_f1 | 0.2665 |

| Lớp | Precision | Recall | F1 |
|---|---|---|---|
| 0 Tiêu cực | 0.3592 | 0.7612 | 0.4880 |
| 1 Trung tính | 0.2857 | 0.0250 | 0.0460 |
| 2 Tích cực | 0.3333 | 0.3191 | 0.3261 |

Ma trận nhầm lẫn (hàng = nhãn thật, cột = dự đoán):

| Nhãn | 0 | 1 | 2 |
|---|---|---|---|
| 0 | 51 | 2 | 14 |
| 1 | 62 | 2 | 16 |
| 2 | 29 | 3 | 15 |
