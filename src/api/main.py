"""
FastAPI phục vụ dữ liệu tầng Gold cho Dashboard và các ứng dụng khác.

Đọc các bảng Gold Parquet (bản đồng bộ từ HDFS về data/lakehouse/gold bởi
scripts/run_all_pipeline.ps1). Hai lớp cache giữ độ trễ < 200 ms:
  - Bảng Gold nạp 1 lần vào bộ nhớ, tự nạp lại khi file Parquet thay đổi
    (kiểm tra thay đổi tối đa 5 giây/lần).
  - Kết quả JSON của từng endpoint + bộ tham số được cache, xóa khi bảng Gold được nạp lại.

Chạy:  uvicorn src.api.main:app --port 8000
Docs:  http://localhost:8000/docs
"""
import base64
import csv
import functools
import json
import os
import threading
import time
import urllib.error
import urllib.request
from datetime import date, datetime
from typing import Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, Field

GOLD_DIR = os.environ.get("GOLD_DIR", os.path.join("data", "lakehouse", "gold"))
FEEDBACK_FILE = os.environ.get("FEEDBACK_FILE", os.path.join("data", "writeback", "feedback.csv"))
AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://127.0.0.1:8085")


def _load_dotenv(path=".env"):
    """Nạp biến môi trường từ file .env ở thư mục gốc dự án (không ghi đè biến đã có)."""
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv()
AIRFLOW_AUTH = f"{os.environ.get('AIRFLOW_ADMIN_USER', '')}:{os.environ.get('AIRFLOW_ADMIN_PASSWORD', '')}"
PIPELINE_DAGS = [
    "bd05_1_ingestion", "bd05_2_etl_clean", "bd05_3_ml_train",
    "bd05_4_scoring_gold", "bd05_5_advanced_analytics", "bd05_6_streaming",
    "lakehouse_master_pipeline",
]
LABEL_NAMES = {0: "Tiêu cực", 1: "Trung tính", 2: "Tích cực"}

TABLES = {
    "trend": "xu_huong_ngay.parquet",
    "alert": "canh_bao_dot_bien.parquet",
    "etl_stats": "etl_quality_stats.parquet",
    "model_metrics": "sentiment_model_metrics.parquet",
    "lda_topics": "lda_chu_de.parquet",
    "lda_daily": "lda_chu_de_theo_ngay.parquet",
    "entity_alert": "canh_bao_thuc_the.parquet",
    "entity_top": "thuc_the_noi_bat.parquet",
    "articles": "bai_viet_tieu_bieu.parquet",
    "delta_history": "delta_lich_su.parquet",
    "stream_stats": "streaming_thong_ke.parquet",
}
REQUIRED = ("trend", "alert")
CHECK_INTERVAL_S = 5

try:
    import orjson

    def dumps(obj):
        return orjson.dumps(obj, option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NON_STR_KEYS)
except ImportError:
    def dumps(obj):
        return json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")


class GoldStore:
    """Bộ nhớ đệm các bảng Gold, tự nạp lại khi thư mục Parquet có thay đổi."""

    def __init__(self, gold_dir):
        self.gold_dir = gold_dir
        self.lock = threading.Lock()
        self.tables = {}
        self.signature = None
        self.loaded_at = None
        self.last_check = 0.0
        self.results = {}

    def _signature(self):
        sig = []
        for name in TABLES.values():
            path = os.path.join(self.gold_dir, name)
            if not os.path.exists(path):
                sig.append((name, None))
                continue
            latest = os.path.getmtime(path)
            for root, _, files in os.walk(path):
                for f in files:
                    latest = max(latest, os.path.getmtime(os.path.join(root, f)))
            sig.append((name, latest))
        return tuple(sig)

    def get(self):
        now = time.monotonic()
        if self.signature is not None and now - self.last_check < CHECK_INTERVAL_S:
            return self.tables
        sig = self._signature()
        self.last_check = now
        if sig != self.signature:
            with self.lock:
                if sig != self.signature:
                    self.tables = self._load()
                    self.results = {}
                    self.signature = sig
                    self.loaded_at = time.strftime("%Y-%m-%d %H:%M:%S")
        return self.tables

    def _load(self):
        tables = {}
        for key, name in TABLES.items():
            path = os.path.join(self.gold_dir, name)
            if os.path.exists(path):
                df = pd.read_parquet(path)
                if "crawled_date" in df.columns:
                    df["crawled_date"] = pd.to_datetime(df["crawled_date"].astype(str)).dt.date
                tables[key] = df
        return tables


store = GoldStore(GOLD_DIR)

app = FastAPI(
    title="Lakehouse Tin tức Tiếng Việt - Gold API",
    description="Xu hướng, cảm xúc và cảnh báo đột biến từ tầng Gold.",
    version="1.0.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)


@app.middleware("http")
async def add_process_time(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    response.headers["X-Process-Time-Ms"] = f"{(time.perf_counter() - start) * 1000:.2f}"
    return response


def cached(fn):
    """Cache JSON đã serialize theo (endpoint, tham số); bị xóa khi Gold được nạp lại."""
    @functools.wraps(fn)
    def wrapper(**kwargs):
        store.get()
        key = (fn.__name__, tuple(sorted((k, str(v)) for k, v in kwargs.items())))
        body = store.results.get(key)
        if body is None:
            body = dumps(fn(**kwargs))
            store.results[key] = body
        return Response(content=body, media_type="application/json")
    return wrapper


def gold():
    tables = store.get()
    missing = [k for k in REQUIRED if k not in tables]
    if missing:
        raise HTTPException(
            status_code=503,
            detail=f"Chưa có bảng Gold {missing} trong {GOLD_DIR}. Hãy chạy scripts/run_all_pipeline.ps1 trước.",
        )
    return tables


def apply_filters(df, start, end, source=None, topic=None):
    mask = pd.Series(True, index=df.index)
    if start:
        mask &= df["crawled_date"] >= start
    if end:
        mask &= df["crawled_date"] <= end
    if source and "source" in df.columns:
        mask &= df["source"] == source
    if topic and "topic" in df.columns:
        mask &= df["topic"] == topic
    return df[mask]


def records(df):
    out = df.copy()
    for c in out.columns:
        if c == "crawled_date":
            out[c] = out[c].astype(str)
    return out.to_dict(orient="records")


def sentiment_summary(df):
    total = int(df["tong_so_bai"].sum())
    pos = int(df["so_bai_tich_cuc"].sum())
    neu = int(df["so_bai_trung_tinh"].sum())
    neg = int(df["so_bai_tieu_cuc"].sum())
    rate = (lambda x: round(x / total * 100, 2) if total else 0.0)
    return {
        "tong_so_bai": total,
        "tich_cuc": pos, "trung_tinh": neu, "tieu_cuc": neg,
        "ty_le_tich_cuc": rate(pos), "ty_le_trung_tinh": rate(neu), "ty_le_tieu_cuc": rate(neg),
    }


SENT_COLS = ["tong_so_bai", "so_bai_tich_cuc", "so_bai_trung_tinh", "so_bai_tieu_cuc"]


def add_rates(df):
    df = df.copy()
    for col, name in [("so_bai_tich_cuc", "ty_le_tich_cuc"), ("so_bai_trung_tinh", "ty_le_trung_tinh"),
                      ("so_bai_tieu_cuc", "ty_le_tieu_cuc")]:
        df[name] = (df[col] / df["tong_so_bai"] * 100).round(2)
    return df


@app.get("/api/health")
def health():
    tables = store.get()
    return {
        "status": "ok" if all(k in tables for k in REQUIRED) else "missing_gold",
        "gold_dir": os.path.abspath(GOLD_DIR),
        "tables": {k: len(v) for k, v in tables.items()},
        "loaded_at": store.loaded_at,
    }


@app.get("/api/overview")
@cached
def overview():
    """Số liệu tổng quan: số bài, khoảng thời gian, nguồn, chuyên mục, cảm xúc, cảnh báo, chất lượng ETL."""
    t = gold()
    trend, alert = t["trend"], t["alert"]
    result = {
        "khoang_thoi_gian": {
            "tu_ngay": str(trend["crawled_date"].min()),
            "den_ngay": str(trend["crawled_date"].max()),
            "so_ngay": int(trend["crawled_date"].nunique()),
        },
        "so_nguon_bao": int(trend["source"].nunique()),
        "so_chuyen_muc": int(trend["topic"].nunique()),
        "cam_xuc": sentiment_summary(trend),
        "canh_bao": {
            "tong_su_kien": int((alert["muc_do_canh_bao"] != "Bình thường").sum()),
            "khung_hoang": int(alert["muc_do_canh_bao"].str.startswith("Khủng hoảng").sum()),
        },
        "top_nguon_bao": records(
            trend.groupby("source", as_index=False)["tong_so_bai"].sum()
            .sort_values("tong_so_bai", ascending=False).head(10)
        ),
        "top_chuyen_muc": records(
            trend.groupby("topic", as_index=False)["tong_so_bai"].sum()
            .sort_values("tong_so_bai", ascending=False).head(10)
        ),
    }
    if "etl_stats" in t and len(t["etl_stats"]):
        result["chat_luong_etl"] = records(t["etl_stats"])[0]
    if "model_metrics" in t and len(t["model_metrics"]):
        mm = t["model_metrics"]
        result["mo_hinh_cam_xuc"] = {
            ds: dict(zip(g["metric"], g["value"].round(4))) for ds, g in mm.groupby("dataset")
        }
    return result


@app.get("/api/trends")
@cached
def trends(
    start: Optional[date] = None,
    end: Optional[date] = None,
    source: Optional[str] = None,
    topic: Optional[str] = None,
    group_by: str = Query("day", pattern="^(day|topic|source|day_topic|day_source)$"),
):
    """Số bài theo ngày / chuyên mục / nguồn báo (có thể kết hợp ngày + chuyên mục hoặc ngày + nguồn)."""
    df = apply_filters(gold()["trend"], start, end, source, topic)
    keys = {
        "day": ["crawled_date"],
        "topic": ["topic"],
        "source": ["source"],
        "day_topic": ["crawled_date", "topic"],
        "day_source": ["crawled_date", "source"],
    }[group_by]
    agg = df.groupby(keys, as_index=False)[SENT_COLS].sum()
    agg = agg.sort_values(keys if "crawled_date" in keys else "tong_so_bai", ascending="crawled_date" in keys)
    return {"group_by": group_by, "so_dong": len(agg), "data": records(add_rates(agg))}


@app.get("/api/sentiment")
@cached
def sentiment(
    start: Optional[date] = None,
    end: Optional[date] = None,
    source: Optional[str] = None,
    topic: Optional[str] = None,
):
    """Phân bố cảm xúc tổng, theo ngày và theo chuyên mục."""
    df = apply_filters(gold()["trend"], start, end, source, topic)
    by_day = df.groupby("crawled_date", as_index=False)[SENT_COLS].sum().sort_values("crawled_date")
    by_topic = df.groupby("topic", as_index=False)[SENT_COLS].sum().sort_values("tong_so_bai", ascending=False)
    by_source = df.groupby("source", as_index=False)[SENT_COLS].sum().sort_values("tong_so_bai", ascending=False)
    return {
        "tong_quan": sentiment_summary(df),
        "theo_ngay": records(add_rates(by_day)),
        "theo_chuyen_muc": records(add_rates(by_topic)),
        "theo_nguon_bao": records(add_rates(by_source)),
    }


@app.get("/api/alerts")
@cached
def alerts(
    start: Optional[date] = None,
    end: Optional[date] = None,
    topic: Optional[str] = None,
    min_z: float = Query(1.5, description="Ngưỡng Z-score tối thiểu (volume hoặc crisis)"),
    include_normal: bool = False,
    limit: int = Query(100, ge=1, le=1000),
):
    """Các sự kiện đột biến Z-score (gold.canh_bao_dot_bien), sắp xếp theo mức nghiêm trọng."""
    df = apply_filters(gold()["alert"], start, end, topic=topic)
    if not include_normal:
        df = df[(df["z_score_volume"] >= min_z) | (df["z_score_crisis"] >= min_z)]
    df = df.assign(_z=df[["z_score_volume", "z_score_crisis"]].max(axis=1)) \
        .sort_values(["_z", "crawled_date"], ascending=[False, False]).drop(columns="_z")
    return {"min_z": min_z, "tong_so": len(df), "data": records(df.head(limit))}


# ---------------------------------------------------------------------------
# Phân tích nâng cao: chủ đề LDA, thực thể, bài viết, Delta Lake, streaming
# ---------------------------------------------------------------------------
def optional_table(name):
    t = store.get()
    if name not in t:
        raise HTTPException(status_code=503,
                            detail=f"Chưa có bảng Gold '{TABLES[name]}'. Hãy chạy DAG bd05_5_advanced_analytics.")
    return t[name]


@app.get("/api/topics")
@cached
def topics():
    """Chủ đề LDA (từ khóa) và số bài theo ngày của từng chủ đề trội."""
    topics_df = optional_table("lda_topics").sort_values("topic_id")
    daily = optional_table("lda_daily").sort_values(["crawled_date", "topic_id"])
    return {"chu_de": records(topics_df), "theo_ngay": records(daily)}


@app.get("/api/entities")
@cached
def entities(
    entity: Optional[str] = None,
    min_z: float = Query(2.0, description="Ngưỡng Z-score cảnh báo"),
    limit: int = Query(50, ge=1, le=500),
):
    """Đột biến theo thực thể/thương hiệu (gold.canh_bao_thuc_the) và top thực thể."""
    top = optional_table("entity_top")
    alert = optional_table("entity_alert")
    if entity:
        series = alert[alert["entity"] == entity].sort_values("crawled_date")
        return {"entity": entity, "theo_ngay": records(series)}
    hot = alert[alert["z_score"] >= min_z].sort_values("z_score", ascending=False).head(limit)
    return {"top_thuc_the": records(top.head(limit)), "dot_bien": records(hot)}


@app.get("/api/articles")
@cached
def articles(
    start: Optional[date] = None,
    end: Optional[date] = None,
    source: Optional[str] = None,
    topic: Optional[str] = None,
    sentiment: Optional[str] = Query(None, description="Tích cực | Trung tính | Tiêu cực"),
    only_alert: bool = False,
    q: Optional[str] = Query(None, description="Tìm trong tiêu đề"),
    limit: int = Query(50, ge=1, le=500),
):
    """Bài viết tiêu biểu (thuộc các đột biến + mẫu ngẫu nhiên) để xem chi tiết và sửa nhãn."""
    df = apply_filters(optional_table("articles"), start, end, source, topic)
    if sentiment:
        df = df[df["sentiment_label"] == sentiment]
    if only_alert:
        df = df[df["thuoc_canh_bao"]]
    if q:
        df = df[df["title"].str.contains(q, case=False, regex=False)]
    return {"tong_so": len(df), "data": records(df.sort_values("crawled_date", ascending=False).head(limit))}


@app.get("/api/lakehouse/delta-history")
@cached
def delta_history():
    """Lịch sử phiên bản bảng Delta silver/bai_viet_delta (time-travel)."""
    return {"data": records(optional_table("delta_history").sort_values("version", ascending=False))}


@app.get("/api/streaming/stats")
@cached
def streaming_stats():
    """Thống kê các micro-batch Spark Structured Streaming từ Kafka."""
    df = optional_table("stream_stats").sort_values("processed_at", ascending=False)
    df = df.assign(processed_at=df["processed_at"].astype(str))
    return {"tong_so_bai": int(df["so_bai"].sum()), "so_batch": len(df), "data": records(df.head(100))}


# ---------------------------------------------------------------------------
# Write-back: người dùng sửa nhãn cảm xúc trên Dashboard
# ---------------------------------------------------------------------------
class Feedback(BaseModel):
    id: int
    label: int = Field(ge=0, le=2, description="0 = Tiêu cực, 1 = Trung tính, 2 = Tích cực")
    note: str = ""
    user: str = "dashboard"


FEEDBACK_FIELDS = ["id", "label", "label_name", "note", "user", "updated_at"]
feedback_lock = threading.Lock()


@app.post("/api/feedback")
def add_feedback(fb: Feedback):
    """
    Ghi nhãn sửa tay vào data/writeback/feedback.csv. DAG bd05_5_advanced_analytics đưa file lên
    HDFS (labels/feedback.csv), MERGE vào bảng Delta, và lần train sau dùng làm dữ liệu huấn luyện.
    """
    os.makedirs(os.path.dirname(FEEDBACK_FILE), exist_ok=True)
    row = {
        "id": fb.id, "label": fb.label, "label_name": LABEL_NAMES[fb.label],
        "note": " ".join(fb.note.split())[:500], "user": fb.user[:50],
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with feedback_lock:
        new_file = not os.path.exists(FEEDBACK_FILE)
        with open(FEEDBACK_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FEEDBACK_FIELDS)
            if new_file:
                w.writeheader()
            w.writerow(row)
    return {"status": "saved", "feedback": row}


@app.get("/api/feedback")
def list_feedback(limit: int = Query(100, ge=1, le=1000)):
    """Các nhãn đã sửa tay (mới nhất trước)."""
    if not os.path.exists(FEEDBACK_FILE):
        return {"tong_so": 0, "data": []}
    df = pd.read_csv(FEEDBACK_FILE).sort_values("updated_at", ascending=False)
    return {"tong_so": len(df), "data": df.head(limit).fillna("").to_dict(orient="records")}


# ---------------------------------------------------------------------------
# Điều phối: trigger DAG Airflow từ web (qua Airflow REST API)
# ---------------------------------------------------------------------------
def airflow_request(method, path, body=None):
    req = urllib.request.Request(
        f"{AIRFLOW_URL}/api/v1{path}", method=method,
        data=json.dumps(body).encode("utf-8") if body is not None else None,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Basic " + base64.b64encode(AIRFLOW_AUTH.encode()).decode(),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        raise HTTPException(status_code=e.code, detail=f"Airflow: {e.read().decode('utf-8', 'replace')[:300]}")
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise HTTPException(status_code=503,
                            detail=f"Không kết nối được Airflow tại {AIRFLOW_URL}: {getattr(e, 'reason', e)}")


@app.get("/api/pipeline/dags")
def pipeline_dags():
    """Trạng thái lần chạy gần nhất của các DAG trong dự án."""
    # 1 request lấy các lần chạy gần nhất của mọi DAG ("~" = tất cả DAG)
    runs = airflow_request("GET", "/dags/~/dagRuns?order_by=-start_date&limit=100").get("dag_runs", [])
    latest = {}
    for r in runs:
        latest.setdefault(r["dag_id"], r)
    out = []
    for dag_id in PIPELINE_DAGS:
        last = latest.get(dag_id, {})
        out.append({"dag_id": dag_id, "state": last.get("state"), "run_id": last.get("dag_run_id"),
                    "start_date": last.get("start_date"), "end_date": last.get("end_date")})
    return {"data": out}


@app.post("/api/pipeline/{dag_id}/trigger")
def trigger_dag(dag_id: str):
    """Kích hoạt một DAG Airflow (nút "Chạy" trên Dashboard)."""
    if dag_id not in PIPELINE_DAGS:
        raise HTTPException(status_code=404, detail=f"DAG không hợp lệ: {dag_id}")
    airflow_request("PATCH", f"/dags/{dag_id}", {"is_paused": False})
    run = airflow_request("POST", f"/dags/{dag_id}/dagRuns", {"conf": {"triggered_by": "dashboard"}})
    return {"dag_id": dag_id, "run_id": run.get("dag_run_id"), "state": run.get("state")}


@app.post("/api/streaming/replay")
def streaming_replay(limit: int = Query(100, ge=1, le=5000), rate: float = Query(50.0, gt=0)):
    """Chạy Kafka Producer Replay ở nền (phát lại `limit` bài vào topic news-topic)."""
    import subprocess
    import sys

    os.makedirs(os.path.join("data", "logs"), exist_ok=True)
    log = open(os.path.join("data", "logs", "kafka_producer.log"), "a", encoding="utf-8")
    # Bắt đầu từ một ngày ngẫu nhiên để mỗi lần phát lại có bài khác nhau
    start_day = f"2022-06-{int(time.time()) % 23 + 7:02d}"
    subprocess.Popen(
        [sys.executable, "-m", "src.streaming.kafka_producer_news", "--limit", str(limit), "--rate", str(rate),
         "--start", start_day],
        stdout=log, stderr=subprocess.STDOUT, env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )
    return {"status": "started", "message": f"Đang phát lại {limit} bài từ {start_day} vào Kafka ({rate:g} bài/giây)"}
