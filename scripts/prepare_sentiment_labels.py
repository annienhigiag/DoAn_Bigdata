"""
Chuẩn bị tập nhãn cảm xúc thật cho Spark MLlib từ UIT-VSFC
(Vietnamese Students' Feedback Corpus, ~16.000 câu gán nhãn tay, Nguyen et al., KSE 2018).

Nhãn UIT-VSFC trùng quy ước của dự án: 0 = Tiêu cực, 1 = Trung tính, 2 = Tích cực.

  python scripts/prepare_sentiment_labels.py
    -> data/labels/sentiment_train.csv  (cột text,label)
Sau đó run_all_pipeline.ps1 (bước 2) tự đưa file lên hdfs:///lakehouse/bd05_tin_tuc/labels/.
"""
import os
import urllib.request

import pandas as pd

BASE_URL = ("https://huggingface.co/datasets/uitnlp/vietnamese_students_feedback/"
            "resolve/refs%2Fconvert%2Fparquet/default/{split}/0000.parquet")
RAW_DIR = os.path.join("data", "labels", "uit_vsfc")
OUT_FILE = os.path.join("data", "labels", "sentiment_train.csv")


def main():
    os.makedirs(RAW_DIR, exist_ok=True)
    frames = []
    for split in ["train", "validation", "test"]:
        path = os.path.join(RAW_DIR, f"{split}.parquet")
        if not os.path.exists(path):
            print(f"Đang tải UIT-VSFC {split}...")
            urllib.request.urlretrieve(BASE_URL.format(split=split), path)
        frames.append(pd.read_parquet(path))

    df = pd.concat(frames, ignore_index=True)
    df = df.rename(columns={"sentence": "text", "sentiment": "label"})[["text", "label"]]
    df["text"] = df["text"].astype(str).str.strip()
    df = df[df["text"].str.len() > 0].drop_duplicates("text")
    df.to_csv(OUT_FILE, index=False, encoding="utf-8")

    print(f"-> Đã ghi {len(df):,} câu vào {OUT_FILE}")
    print("   Phân bố nhãn:", df["label"].value_counts().sort_index().to_dict())


if __name__ == "__main__":
    main()
