"""
Kafka Producer Replay: phát lại các bài báo trong dataset vào Kafka topic theo đúng thứ tự
thời gian crawl, mô phỏng luồng tin tức thời gian thực.

  python -m src.streaming.kafka_producer_news --limit 500 --rate 50
  python -m src.streaming.kafka_producer_news --start 2022-06-20 --speedup 3600

  --rate     : số bài/giây (mặc định 20); bỏ qua nếu dùng --speedup
  --speedup  : phát theo khoảng cách thời gian thật giữa các bài, tua nhanh N lần
               (3600 = 1 giờ dữ liệu phát trong 1 giây)
Message: key = nguồn báo (để cùng nguồn vào cùng partition), value = JSON bài báo.
"""
import argparse
import json
import time

import pandas as pd
from kafka import KafkaProducer


def get_kafka_producer(bootstrap_servers="localhost:9094"):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=20,
    )


def stream_news_events(parquet_path="data/filtered_2021_2022/news_2021_2022.parquet", topic="news-topic",
                       limit=200, rate=20.0, speedup=None, start=None, bootstrap_servers="localhost:9094"):
    print(" KAFKA PRODUCER REPLAY")
    print(f" Broker: {bootstrap_servers} | Topic: {topic}")

    df = pd.read_parquet(parquet_path, columns=["id", "title", "content", "source", "topic", "url", "crawled_at"])
    df = df.sort_values("crawled_at")
    if start:
        df = df[df["crawled_at"] >= pd.Timestamp(start)]
    df = df.head(limit)
    print(f"-> Phát lại {len(df):,} bài từ {df['crawled_at'].min()} đến {df['crawled_at'].max()}")

    producer = get_kafka_producer(bootstrap_servers)
    prev_ts = None
    t0 = time.time()
    for i, row in enumerate(df.itertuples(index=False), 1):
        if speedup and prev_ts is not None:
            time.sleep(max(0.0, (row.crawled_at - prev_ts).total_seconds() / speedup))
        elif not speedup and rate > 0:
            time.sleep(1.0 / rate)
        prev_ts = row.crawled_at

        event = {
            "id": int(row.id),
            "title": str(row.title),
            "content": str(row.content),
            "source": str(row.source),
            "topic": str(row.topic),
            "url": str(row.url),
            "crawled_at": row.crawled_at.isoformat(),
        }
        producer.send(topic, key=event["source"], value=event)
        if i % 50 == 0 or i == len(df):
            print(f"   đã phát {i:,}/{len(df):,} bài ({i / (time.time() - t0):.1f} bài/giây)")

    producer.flush()
    print(f"-> Hoàn tất phát {len(df):,} sự kiện vào '{topic}'")


def main():
    ap = argparse.ArgumentParser(description="Kafka Producer Replay tin tức")
    ap.add_argument("--parquet", default="data/filtered_2021_2022/news_2021_2022.parquet")
    ap.add_argument("--topic", default="news-topic")
    ap.add_argument("--bootstrap", default="localhost:9094")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--rate", type=float, default=20.0)
    ap.add_argument("--speedup", type=float, default=None)
    ap.add_argument("--start", default=None, help="Bắt đầu phát từ thời điểm (YYYY-MM-DD)")
    a = ap.parse_args()
    stream_news_events(a.parquet, a.topic, a.limit, a.rate, a.speedup, a.start, a.bootstrap)


if __name__ == "__main__":
    main()
