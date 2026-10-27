import json
import os
import urllib.error
import urllib.request

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import theme as T

# Cấu hình trang
st.set_page_config(
    page_title="Lakehouse Media Trend & Sentiment Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)
T.register_plotly_template()
T.inject_css()

# Tải dữ liệu từ tầng Gold
@st.cache_data
def load_gold_data():
    trend_path = os.path.join("data", "lakehouse", "gold", "xu_huong_ngay.parquet")
    alert_path = os.path.join("data", "lakehouse", "gold", "canh_bao_dot_bien.parquet")
    
    if not os.path.exists(trend_path) or not os.path.exists(alert_path):
        st.error("Không tìm thấy dữ liệu tầng Gold. Vui lòng chạy job ETL trước!")
        return None, None
        
    df_trend = pd.read_parquet(trend_path)
    df_alert = pd.read_parquet(alert_path)
    
    df_trend["crawled_date"] = pd.to_datetime(df_trend["crawled_date"].astype(str))
    df_alert["crawled_date"] = pd.to_datetime(df_alert["crawled_date"].astype(str))
    
    # Chuẩn hóa tên cột phục vụ hiển thị
    if "z_score_volume" in df_alert.columns and "z_score" not in df_alert.columns:
        df_alert["z_score"] = df_alert["z_score_volume"]
    if "tb_so_bai_chuyen_muc" in df_alert.columns and "trung_binh_lich_su" not in df_alert.columns:
        df_alert["trung_binh_lich_su"] = df_alert["tb_so_bai_chuyen_muc"]
    if "do_lech_chuan_so_bai" in df_alert.columns and "do_lech_chuan" not in df_alert.columns:
        df_alert["do_lech_chuan"] = df_alert["do_lech_chuan_so_bai"]
    
    return df_trend, df_alert


GOLD_DIR = os.path.join("data", "lakehouse", "gold")
API_URL = os.environ.get("LAKEHOUSE_API", "http://127.0.0.1:8000")
LABEL_OPTIONS = {"Tiêu cực": 0, "Trung tính": 1, "Tích cực": 2}


@st.cache_data(show_spinner=False)
def _read_table(path, mtime):
    df = pd.read_parquet(path)
    if "crawled_date" in df.columns:
        df["crawled_date"] = pd.to_datetime(df["crawled_date"].astype(str)).dt.date
    return df


def load_table(name):
    """Đọc 1 bảng Gold; cache theo thời điểm sửa file để tự cập nhật sau khi đồng bộ Gold."""
    path = os.path.join(GOLD_DIR, name)
    if not os.path.exists(path):
        return None
    mtime = max([os.path.getmtime(os.path.join(r, f)) for r, _, fs in os.walk(path) for f in fs] or [os.path.getmtime(path)])
    return _read_table(path, mtime)


def call_api(method, path, payload=None):
    """Gọi FastAPI (write-back, trigger DAG). Trả về (ok, dữ liệu hoặc thông báo lỗi)."""
    req = urllib.request.Request(
        API_URL + path, method=method,
        data=json.dumps(payload).encode("utf-8") if payload is not None else None,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return True, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return False, e.read().decode("utf-8", "replace")[:300]
    except urllib.error.URLError as e:
        return False, f"Không kết nối được API {API_URL} ({e.reason}). Chạy scripts/run_api.ps1."


@st.cache_data(ttl=15, show_spinner=False)
def dag_status():
    """Trạng thái DAG từ Airflow, cache 15 giây để không làm chậm mỗi lần tải trang."""
    return call_api("GET", "/api/pipeline/dags")


def render_pipeline_tab():
    T.section("Điều phối pipeline (Airflow) từ web")
    st.caption("Nút Chạy gọi FastAPI → Airflow REST API để kích hoạt DAG. Theo dõi chi tiết tại http://localhost:8085")
    ok, res = dag_status()
    if ok:
        dag_df = pd.DataFrame(res["data"])
        for _, row in dag_df.iterrows():
            c1, c2, c3 = st.columns([3, 2, 1])
            c1.markdown(f"**{row['dag_id']}**")
            c2.markdown(f"Lần chạy gần nhất: `{row['state'] or 'chưa chạy'}`")
            if c3.button("▶ Chạy", key=f"run_{row['dag_id']}"):
                ok_run, r = call_api("POST", f"/api/pipeline/{row['dag_id']}/trigger")
                if ok_run:
                    dag_status.clear()
                    st.success(f"Đã kích hoạt {row['dag_id']} ({r.get('run_id')})")
                else:
                    st.error(r)
    else:
        st.warning(res)

    T.section("Luồng streaming Kafka → Spark Structured Streaming")
    c1, c2 = st.columns([1, 3])
    n_replay = c1.number_input("Số bài phát lại", min_value=10, max_value=2000, value=100, step=10)
    if c1.button("📡 Phát lại vào Kafka"):
        ok_rp, r = call_api("POST", f"/api/streaming/replay?limit={int(n_replay)}")
        if ok_rp:
            c1.success(r.get("message"))
        else:
            c1.error(r)
    stream_stats = load_table("streaming_thong_ke.parquet")
    if stream_stats is not None and len(stream_stats):
        stream_stats = stream_stats.sort_values("processed_at")
        c2.metric("Tổng bài đã xử lý qua streaming", f"{int(stream_stats['so_bai'].sum()):,}",
                  f"{len(stream_stats)} micro-batch", delta_color="off")
        c2.dataframe(stream_stats.sort_values("processed_at", ascending=False).head(10), width="stretch", hide_index=True)
    else:
        c2.info("Chưa có micro-batch nào. Phát lại tin vào Kafka rồi chạy DAG bd05_6_streaming.")

    T.section("✍️ Write-back: sửa nhãn cảm xúc")
    st.caption("Nhãn sửa được ghi qua API vào data/writeback/feedback.csv → DAG 5 MERGE vào bảng Delta, "
               "DAG 3 dùng làm dữ liệu huấn luyện lần sau.")
    arts = load_table("bai_viet_tieu_bieu.parquet")
    if arts is not None:
        c1, c2 = st.columns([2, 1])
        q = c1.text_input("Tìm bài theo tiêu đề:", "")
        sent_filter = c2.selectbox("Lọc theo nhãn hiện tại:", ["Tất cả"] + list(LABEL_OPTIONS))
        view = arts
        if q:
            view = view[view["title"].str.contains(q, case=False, regex=False)]
        if sent_filter != "Tất cả":
            view = view[view["sentiment_label"] == sent_filter]
        view = view.head(300)
        if len(view):
            choice = st.selectbox("Chọn bài:", view.index,
                                  format_func=lambda i: f"[{view.at[i, 'sentiment_label']}] {view.at[i, 'title'][:110]}")
            art = view.loc[choice]
            st.markdown(f"**{art['title']}**  \n{art['source']} • {art['crawled_date']} • {art['topic']}  \n_{art['sapo']}…_")
            st.markdown(f"Nhãn mô hình: **{art['sentiment_label']}** (độ tin cậy {art['sentiment_score']:.2f}) • [Mở bài]({art['url']})")
            with st.form("feedback_form", clear_on_submit=True):
                new_label = st.radio("Nhãn đúng:", list(LABEL_OPTIONS), horizontal=True,
                                     index=int(art["sentiment_class"]) if art["sentiment_class"] in (0, 1, 2) else 1)
                note = st.text_input("Ghi chú (không bắt buộc):")
                if st.form_submit_button("💾 Lưu nhãn"):
                    ok_fb, r = call_api("POST", "/api/feedback",
                                        {"id": int(art["id"]), "label": LABEL_OPTIONS[new_label], "note": note})
                    if ok_fb:
                        st.success(f"Đã lưu: bài {art['id']} → {new_label}")
                    else:
                        st.error(r)
        else:
            st.info("Không có bài phù hợp.")
        ok_l, fb = call_api("GET", "/api/feedback?limit=20")
        if ok_l and fb["tong_so"]:
            st.markdown(f"**{fb['tong_so']} nhãn đã sửa** (mới nhất):")
            st.dataframe(pd.DataFrame(fb["data"]), width="stretch", hide_index=True)

    T.section("🕰️ Delta Lake: lịch sử phiên bản (time-travel)")
    hist = load_table("delta_lich_su.parquet")
    if hist is not None:
        st.dataframe(hist.sort_values("version", ascending=False)[["version", "timestamp", "operation", "operation_metrics"]],
                     width="stretch", hide_index=True)
    else:
        st.info("Chưa có bảng Delta. Chạy DAG bd05_5_advanced_analytics.")

    with st.expander("🏗️ Kiến trúc hệ thống"):
        st.markdown("""
```text
Kaggle JSON ─filter─> Parquet ─DAG1─> HDFS Bronze (3 DataNodes, replication 3)
Kafka Producer Replay ─> Kafka (+Zookeeper) ─DAG6 Spark Structured Streaming─> Bronze/Silver streaming
Bronze ─DAG2 ETL (HTML, NFC, SHA-256 + SimHash)─> Silver bai_viet_clean (theo ngày)
UIT-VSFC + nhãn write-back ─DAG3 MLlib TF-IDF + LR─> models/sentiment_model
Silver ─DAG4 scoring + Spark SQL + Z-score─> Gold xu_huong_ngay, canh_bao_dot_bien
Silver ─DAG5 Delta Lake (MERGE/OPTIMIZE/VACUUM) + thực thể + LDA─> Gold nâng cao
Gold ─sync─> FastAPI (cache, < 200 ms) + Dashboard Streamlit (6 màn hình, write-back)
```
""")
        st.caption("Đồ án môn Dữ liệu lớn (Big Data) • Đại học • Nhóm 4 thành viên")


LEVEL_COLORS = {"Khủng hoảng / Bùng nổ": T.NEGATIVE, "Cảnh báo / Tăng cao": T.NEUTRAL}
LEVEL_ICONS = {"Khủng hoảng / Bùng nổ": "🔴", "Cảnh báo / Tăng cao": "🟡"}


@st.cache_data(show_spinner=False)
def build_views(start_d, end_d, selected_source, selected_topic):
    """Lọc dữ liệu và dựng toàn bộ biểu đồ; kết quả được cache theo bộ lọc."""
    df_trend, df_alert = load_gold_data()

    trend_dates = df_trend["crawled_date"].dt.date
    alert_dates = df_alert["crawled_date"].dt.date
    filtered_trend = df_trend[(trend_dates >= start_d) & (trend_dates <= end_d)]
    filtered_alert = df_alert[(alert_dates >= start_d) & (alert_dates <= end_d)]

    if selected_source != "Tất cả":
        filtered_trend = filtered_trend[filtered_trend["source"] == selected_source]
    if selected_topic != "Tất cả":
        filtered_trend = filtered_trend[filtered_trend["topic"] == selected_topic]
        filtered_alert = filtered_alert[filtered_alert["topic"] == selected_topic]

    total = int(filtered_trend["tong_so_bai"].sum())
    pos = int(filtered_trend["so_bai_tich_cuc"].sum())
    neu = int(filtered_trend["so_bai_trung_tinh"].sum())
    neg = int(filtered_trend["so_bai_tieu_cuc"].sum())
    rate = (lambda x: x / total * 100 if total > 0 else 0)
    alerts_only = filtered_alert[filtered_alert["muc_do_canh_bao"] != "Bình thường"] \
        .sort_values("z_score", ascending=False)

    # Chuỗi theo ngày cho sparkline và biểu đồ
    daily = filtered_trend.groupby("crawled_date")[["tong_so_bai", "so_bai_tich_cuc", "so_bai_trung_tinh",
                                                     "so_bai_tieu_cuc"]].sum().reset_index().sort_values("crawled_date")
    share = daily[["so_bai_tich_cuc", "so_bai_trung_tinh", "so_bai_tieu_cuc"]].div(daily["tong_so_bai"].where(daily["tong_so_bai"] > 0), axis=0) * 100
    alerts_per_day = alerts_only.groupby("crawled_date").size().reindex(daily["crawled_date"], fill_value=0)

    # So sánh nửa sau với nửa đầu khoảng thời gian
    half = len(daily) // 2
    first, second = daily["tong_so_bai"].iloc[:half].mean(), daily["tong_so_bai"].iloc[half:].mean()
    growth = (second / first - 1) * 100 if half and first else None

    views = {"kpi": {
        "total": total, "pos": pos, "neu": neu, "neg": neg,
        "pos_rate": rate(pos), "neu_rate": rate(neu), "neg_rate": rate(neg),
        "alerts": len(alerts_only), "growth": growth,
        "spark_total": daily["tong_so_bai"].tolist(),
        "spark_pos": share["so_bai_tich_cuc"].tolist(),
        "spark_neu": share["so_bai_trung_tinh"].tolist(),
        "spark_neg": share["so_bai_tieu_cuc"].tolist(),
        "spark_alerts": alerts_per_day.tolist(),
    }}

    # Điểm nóng: 3 sự kiện có Z-score cao nhất
    views["hot"] = [
        {"title": r.topic, "meta": f"{r.crawled_date:%d/%m/%Y} • {int(r.so_bai_ngay)} bài (TB {r.trung_binh_lich_su:.1f})",
         "z": float(r.z_score), "accent": LEVEL_COLORS.get(r.muc_do_canh_bao, T.NEUTRAL)}
        for r in alerts_only.head(3).itertuples()
    ]

    # Xu hướng theo ngày: vùng tô + đường trung bình trượt 7 ngày + đánh dấu ngày cao nhất
    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=daily["crawled_date"], y=daily["tong_so_bai"], name="Số bài", mode="lines",
        line=dict(color=T.PRIMARY, width=2.5, shape="spline"), fill="tozeroy", fillcolor="rgba(37,99,235,0.12)",
        hovertemplate="%{x|%d/%m}: <b>%{y:,}</b> bài<extra></extra>"))
    fig_trend.add_trace(go.Scatter(
        x=daily["crawled_date"], y=daily["tong_so_bai"].rolling(7, min_periods=1).mean(), name="TB trượt 7 ngày",
        mode="lines", line=dict(color=T.INK, width=1.5, dash="dot"), hovertemplate="TB 7 ngày: %{y:,.0f}<extra></extra>"))
    if len(daily):
        peak = daily.loc[daily["tong_so_bai"].idxmax()]
        fig_trend.add_annotation(x=peak["crawled_date"], y=peak["tong_so_bai"], text=f"Cao nhất {T.num(peak['tong_so_bai'])} bài",
                                 showarrow=True, arrowhead=0, ax=-60, ay=-30, xanchor="right", bgcolor="white", bordercolor=T.GRID,
                                 font=dict(size=12, color=T.INK))
    fig_trend.update_layout(title="Số bài báo xuất bản mỗi ngày", height=380, hovermode="x unified",
                            yaxis_title="Số bài", xaxis_title=None, xaxis_tickformat="%d/%m",
                            legend=dict(x=0.01, y=0.98, xanchor="left", yanchor="top", bgcolor="rgba(255,255,255,0.7)"))
    views["fig_trend"] = fig_trend
    views["peak"] = (peak["crawled_date"], int(peak["tong_so_bai"])) if len(daily) else None

    def hbar(df, label_col, title, color):
        df = df.assign(**{label_col: df[label_col].replace("", "(không rõ)")})
        fig = go.Figure(go.Bar(
            x=df["tong_so_bai"], y=df[label_col], orientation="h", marker=dict(color=color, line=dict(width=0)),
            text=[T.num(v) for v in df["tong_so_bai"]], textposition="outside", cliponaxis=False,
            hovertemplate="%{y}: <b>%{x:,}</b> bài<extra></extra>"))
        fig.update_layout(title=title, height=400, yaxis=dict(autorange="reversed", showgrid=False),
                          xaxis=dict(showticklabels=False, showgrid=False), bargap=0.35, margin=dict(r=40))
        return fig

    topic_summary = filtered_trend.groupby("topic")["tong_so_bai"].sum().reset_index() \
        .sort_values("tong_so_bai", ascending=False).head(10)
    views["fig_topic"] = hbar(topic_summary, "topic", "Top 10 chuyên mục", T.PRIMARY)
    source_summary = filtered_trend.groupby("source")["tong_so_bai"].sum().reset_index() \
        .sort_values("tong_so_bai", ascending=False).head(10)
    views["fig_source"] = hbar(source_summary, "source", "Top 10 nguồn báo", "#0d9488")

    # Donut cảm xúc với tỷ lệ tiêu cực ở giữa
    fig_pie = go.Figure(go.Pie(
        labels=["Tích cực", "Trung tính", "Tiêu cực"], values=[pos, neu, neg], hole=0.68, sort=False,
        marker=dict(colors=[T.POSITIVE, T.NEUTRAL, T.NEGATIVE], line=dict(color="white", width=3)),
        textinfo="percent", textposition="outside", hovertemplate="%{label}: <b>%{value:,}</b> bài (%{percent})<extra></extra>"))
    fig_pie.add_annotation(text=f"<b>{rate(neg):.0f}%</b><br><span style='font-size:12px;color:{T.MUTED}'>tiêu cực</span>",
                           showarrow=False, font=dict(size=26, color=T.NEGATIVE))
    fig_pie.update_layout(title="Cơ cấu cảm xúc", height=380, legend=dict(orientation="h", y=-0.08, x=0.5, xanchor="center"))
    views["fig_pie"] = fig_pie

    # Tỷ trọng cảm xúc theo ngày (100%)
    fig_sent = go.Figure()
    for col, name, color in [("so_bai_tieu_cuc", "Tiêu cực", T.NEGATIVE), ("so_bai_trung_tinh", "Trung tính", T.NEUTRAL),
                             ("so_bai_tich_cuc", "Tích cực", T.POSITIVE)]:
        fig_sent.add_trace(go.Scatter(x=daily["crawled_date"], y=share[col], name=name, mode="lines", stackgroup="one",
                                      line=dict(width=0.5, color=color, shape="spline"),
                                      hovertemplate=f"{name}: %{{y:.1f}}%<extra></extra>"))
    fig_sent.update_layout(title="Tỷ trọng cảm xúc theo ngày", height=380, hovermode="x unified", xaxis_tickformat="%d/%m",
                           legend=dict(orientation="h", y=-0.12, x=0.5, xanchor="center", yanchor="top"),
                           yaxis=dict(title="%", range=[0, 100], ticksuffix="%"))
    views["fig_sent_line"] = fig_sent

    if len(alerts_only) > 0:
        table = alerts_only[["crawled_date", "topic", "so_bai_ngay", "trung_binh_lich_su",
                             "z_score", "z_score_crisis", "muc_do_canh_bao"]].copy()
        table["crawled_date"] = table["crawled_date"].dt.date
        table["muc_do_canh_bao"] = table["muc_do_canh_bao"].map(lambda m: f"{LEVEL_ICONS.get(m, '')} {m}")
        views["alerts_table"] = table
        fig_sc = px.scatter(
            alerts_only, x="crawled_date", y="z_score", size="so_bai_ngay", color="muc_do_canh_bao",
            hover_name="topic", hover_data={"so_bai_ngay": True, "crawled_date": False, "muc_do_canh_bao": False},
            color_discrete_map=LEVEL_COLORS, size_max=36, labels={"crawled_date": "", "z_score": "Z-score", "so_bai_ngay": "Số bài"},
            title="Bản đồ đột biến: mỗi bong bóng là một chuyên mục trong một ngày")
        fig_sc.add_hline(y=2, line_dash="dash", line_color=T.NEGATIVE, opacity=0.5,
                         annotation_text="Ngưỡng bùng nổ Z = 2", annotation_position="top left")
        fig_sc.update_traces(marker=dict(line=dict(width=1, color="white"), opacity=0.85))
        fig_sc.update_layout(height=420, xaxis_tickformat="%d/%m")
        views["fig_scatter"] = fig_sc
    else:
        views["alerts_table"] = None
        views["fig_scatter"] = None
    return views


df_trend, df_alert = load_gold_data()

if df_trend is not None and df_alert is not None:
    # --- THANH ĐIỀU HƯỚNG + TIÊU ĐỀ TRANG ---
    T.navbar([("Tổng quan", "#"), ("Airflow", "http://localhost:8085"), ("HDFS", "http://localhost:9870"),
              ("Spark", "http://localhost:8080"), ("API", "http://localhost:8000/docs")])

    etl_info = load_table("etl_quality_stats.parquet")
    updated = str(etl_info.iloc[0]["run_at"])[:16] if etl_info is not None and len(etl_info) else "chưa có"
    T.page_header(
        "Trang chủ › <b>Bảng điều khiển</b>",
        "Theo dõi Xu hướng & Cảm xúc Tin tức Tiếng Việt",
        "59.168 bài báo tháng 06/2022 • Lakehouse trên HDFS, xử lý bằng Spark, cập nhật qua Airflow",
        updated,
    )

    # --- THANH BỘ LỌC ---
    min_date = df_trend["crawled_date"].min().date()
    max_date = df_trend["crawled_date"].max().date()
    all_sources = ["Tất cả"] + sorted(list(df_trend["source"].unique()))
    all_topics = ["Tất cả"] + sorted(list(df_trend["topic"].unique()))

    f1, f2, f3 = st.columns([2, 1.3, 1.3])
    with f1:
        date_range = st.date_input("📅 Khoảng thời gian", value=(min_date, max_date),
                                   min_value=min_date, max_value=max_date, format="DD/MM/YYYY")
    with f2:
        selected_source = st.selectbox("📰 Nguồn báo", all_sources)
    with f3:
        selected_topic = st.selectbox("🗂️ Chuyên mục", all_topics)

    # Toàn bộ số liệu và biểu đồ được tính trong build_views() có @st.cache_data theo bộ lọc,
    # nên tải lại trang / đổi tab không phải dựng lại biểu đồ.
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
    else:
        start_d, end_d = min_date, max_date

    v = build_views(start_d, end_d, selected_source, selected_topic)

    # --- HÀNG KPI ---
    k = v["kpi"]
    T.kpi_cards([
        {"label": "Tổng số bài báo", "icon": "📰", "value": T.num(k["total"]),
         "sub": f"{(end_d - start_d).days + 1} ngày • nửa sau so với nửa đầu", "accent": T.PRIMARY,
         "delta": k["growth"], "spark": k["spark_total"]},
        {"label": "Tích cực", "icon": "😊", "value": f"{k['pos_rate']:.1f}%", "sub": f"{T.num(k['pos'])} bài",
         "accent": T.POSITIVE, "spark": k["spark_pos"]},
        {"label": "Trung tính", "icon": "😐", "value": f"{k['neu_rate']:.1f}%", "sub": f"{T.num(k['neu'])} bài",
         "accent": T.NEUTRAL, "spark": k["spark_neu"]},
        {"label": "Tiêu cực", "icon": "😟", "value": f"{k['neg_rate']:.1f}%", "sub": f"{T.num(k['neg'])} bài",
         "accent": T.NEGATIVE, "spark": k["spark_neg"]},
        {"label": "Cảnh báo đột biến", "icon": "🚨", "value": T.num(k["alerts"]), "sub": "sự kiện Z-score ≥ 1,5 theo ngày",
         "accent": T.NEGATIVE if k["alerts"] else T.POSITIVE, "spark": k["spark_alerts"]},
    ])

    # --- TABS NỘI DUNG (6 màn hình) ---
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Tổng quan",
        "🎭 Cảm xúc",
        "🚨 Cảnh báo đột biến",
        "🧠 Chủ đề LDA",
        "🏷️ Thực thể & Thương hiệu",
        "⚙️ Pipeline & Write-back",
    ])

    with tab1:
        if v["hot"]:
            T.section("Điểm nóng truyền thông", "3 sự kiện có Z-score cao nhất trong bộ lọc hiện tại")
            T.hot_cards(v["hot"])
        if v["peak"]:
            T.insight(f"Ngày nhiều tin nhất là {v['peak'][0]:%d/%m/%Y} với {T.num(v['peak'][1])} bài"
                      + (f"; lượng tin nửa sau giai đoạn {'tăng' if k['growth'] >= 0 else 'giảm'} "
                         f"{abs(k['growth']):.0f}% so với nửa đầu." if k["growth"] is not None else "."))
        st.plotly_chart(v["fig_trend"], width="stretch")

        col_left, col_right = st.columns(2)
        with col_left:
            st.plotly_chart(v["fig_topic"], width="stretch")
        with col_right:
            st.plotly_chart(v["fig_source"], width="stretch")

        etl = load_table("etl_quality_stats.parquet")
        if etl is not None and len(etl):
            etl = etl.iloc[0]
            T.section("Chất lượng dữ liệu qua pipeline ETL",
                      f"Tỷ lệ trùng lặp đã lọc {etl['duplicate_rate_pct']}% • SimHash Hamming ≤ {int(etl['hamming_threshold'])}")
            stages = ["Bronze (thô)", "Hợp lệ", "Sau khử trùng chính xác", "Silver (sau SimHash)"]
            valid = int(etl["valid_records"])
            after_exact = valid - int(etl["exact_url_duplicates"]) - int(etl["exact_content_duplicates"])
            values = [int(etl["bronze_records"]), valid, after_exact, int(etl["silver_records"])]
            fig_funnel = go.Figure(go.Funnel(
                y=stages, x=values, texttemplate="%{value:,} bài<br>%{percentInitial:.0%}",
                marker=dict(color=[T.MUTED, T.PRIMARY, "#0d9488", T.POSITIVE]),
                connector=dict(line=dict(color=T.GRID, width=1))))
            fig_funnel.update_layout(height=300, title="Từ Bronze đến Silver", margin=dict(l=10, r=10, t=50, b=10))
            st.plotly_chart(fig_funnel, width="stretch")

    with tab2:
        c1, c2 = st.columns([2, 3])
        with c1:
            st.plotly_chart(v["fig_pie"], width="stretch")
        with c2:
            st.plotly_chart(v["fig_sent_line"], width="stretch")

        metrics = load_table("sentiment_model_metrics.parquet")
        if metrics is not None and len(metrics):
            T.section("Chất lượng mô hình Spark MLlib", "pyvi → TF-IDF → Logistic Regression có trọng số lớp")
            m = {(r.dataset, r.metric): r.value for r in metrics.itertuples()}
            cols = st.columns(4)
            with cols[0]:
                T.score_card("Accuracy – tập kiểm thử", m.get(("test_split", "accuracy"), 0),
                             f"{T.num(m.get(('test_split', 'n_samples'), 0))} câu UIT-VSFC", T.PRIMARY)
            with cols[1]:
                T.score_card("Macro-F1 – tập kiểm thử", m.get(("test_split", "macro_f1"), 0),
                             "Trung bình F1 của 3 lớp", T.POSITIVE)
            with cols[2]:
                T.score_card("Accuracy – báo chí gán tay", m.get(("manual_200", "accuracy"), 0),
                             f"{T.num(m.get(('manual_200', 'n_samples'), 0))} bài báo", T.NEUTRAL)
            with cols[3]:
                T.score_card("Macro-F1 – báo chí gán tay", m.get(("manual_200", "macro_f1"), 0),
                             "Độ lệch miền (domain shift)", T.NEGATIVE)
            per_class = metrics[metrics["metric"].str.startswith("f1_class_")].copy()
            if len(per_class):
                per_class["Lớp"] = per_class["metric"].str[-1].map({"0": "Tiêu cực", "1": "Trung tính", "2": "Tích cực"})
                per_class["Tập"] = per_class["dataset"].map({"test_split": "Kiểm thử UIT-VSFC", "manual_200": "Báo chí gán tay"})
                fig_f1 = px.bar(per_class, x="Lớp", y="value", color="Tập", barmode="group", text_auto=".2f",
                                color_discrete_sequence=[T.PRIMARY, T.NEUTRAL], labels={"value": "F1"},
                                title="F1 theo từng lớp: mô hình yếu nhất ở lớp Trung tính")
                fig_f1.update_layout(height=340, yaxis=dict(range=[0, 1]))
                st.plotly_chart(fig_f1, width="stretch")

    with tab3:
        T.section("Phát hiện đột biến truyền thông (Z-score)",
                  "Z = (số bài trong ngày − trung bình của chuyên mục) / độ lệch chuẩn • Z ≥ 2 là bùng nổ")
        if v["alerts_table"] is not None:
            st.plotly_chart(v["fig_scatter"], width="stretch")
            zmax = float(v["alerts_table"]["z_score"].max())
            st.dataframe(
                v["alerts_table"], width="stretch", hide_index=True, height=380,
                column_config={
                    "crawled_date": st.column_config.DateColumn("Ngày", format="DD/MM/YYYY"),
                    "topic": "Chuyên mục",
                    "so_bai_ngay": st.column_config.NumberColumn("Số bài", format="%d"),
                    "trung_binh_lich_su": st.column_config.NumberColumn("TB chuyên mục", format="%.1f"),
                    "z_score": st.column_config.ProgressColumn("Z-score số bài", format="%.2f", min_value=0, max_value=zmax),
                    "z_score_crisis": st.column_config.NumberColumn("Z-score tiêu cực", format="%.2f"),
                    "muc_do_canh_bao": "Mức cảnh báo",
                })
        else:
            st.success("Không có chủ đề nào chạm ngưỡng cảnh báo đột biến trong bộ lọc hiện tại.")

        arts = load_table("bai_viet_tieu_bieu.parquet")
        if arts is not None:
            T.section("Bài viết thuộc các đột biến")
            sel = arts[arts["thuoc_canh_bao"]]
            if selected_topic != "Tất cả":
                sel = sel[sel["topic"] == selected_topic]
            st.dataframe(sel[["crawled_date", "topic", "source", "title", "sentiment_label", "url"]]
                         .sort_values("crawled_date", ascending=False).head(200),
                         width="stretch", hide_index=True,
                         column_config={"crawled_date": st.column_config.DateColumn("Ngày", format="DD/MM/YYYY"),
                                        "topic": "Chuyên mục", "source": "Nguồn", "title": "Tiêu đề",
                                        "sentiment_label": "Cảm xúc", "url": st.column_config.LinkColumn("Link", display_text="Mở bài")})

    with tab4:
        T.section("Chủ đề ẩn trong tin tức (Spark MLlib LDA)", "Mỗi bài được gán chủ đề có xác suất cao nhất")
        topics = load_table("lda_chu_de.parquet")
        lda_daily = load_table("lda_chu_de_theo_ngay.parquet")
        if topics is None or lda_daily is None:
            st.warning("Chưa có kết quả LDA. Chạy DAG bd05_5_advanced_analytics ở tab Pipeline.")
        else:
            topics = topics.sort_values("tong_so_bai", ascending=False)
            T.topic_cards([{"id": int(r.topic_id), "count": int(r.tong_so_bai),
                            "keywords": [w.strip() for w in str(r.tu_khoa).split(",")]} for r in topics.itertuples()],
                          int(topics["tong_so_bai"].max()))
            lda_daily = lda_daily.assign(chu_de="Chủ đề " + lda_daily["topic_id"].astype(str) + ": " + lda_daily["nhan_chu_de"])
            fig = px.area(lda_daily.sort_values("crawled_date"), x="crawled_date", y="so_bai", color="chu_de",
                          title="Số bài theo chủ đề trội mỗi ngày", labels={"crawled_date": "", "so_bai": "Số bài", "chu_de": ""},
                          line_shape="spline")
            fig.update_layout(height=440, xaxis_tickformat="%d/%m",
                              legend=dict(orientation="v", y=1, x=1.02, xanchor="left", yanchor="top"))
            st.plotly_chart(fig, width="stretch")

    with tab5:
        T.section("Đột biến theo thực thể / thương hiệu", "Thực thể trích từ tiêu đề: cụm từ viết hoa và từ điển thương hiệu")
        top_ent = load_table("thuc_the_noi_bat.parquet")
        ent_alert = load_table("canh_bao_thuc_the.parquet")
        if top_ent is None or ent_alert is None:
            st.warning("Chưa có kết quả thực thể. Chạy DAG bd05_5_advanced_analytics ở tab Pipeline.")
        else:
            hot = ent_alert[ent_alert["muc_do_canh_bao"] != "Bình thường"].sort_values("z_score", ascending=False)
            T.hot_cards([{"title": r.entity, "meta": f"{r.crawled_date:%d/%m/%Y} • {int(r.so_bai)} bài (TB {r.tb_so_bai:.1f})",
                          "z": float(r.z_score), "accent": T.NEGATIVE if r.muc_do_canh_bao == "Đột biến mạnh" else T.NEUTRAL}
                         for r in hot.head(3).itertuples()])
            c1, c2 = st.columns([1, 1])
            with c1:
                top20 = top_ent.head(20)
                fig = go.Figure(go.Bar(
                    x=top20["tong_so_bai"], y=top20["entity"], orientation="h",
                    marker=dict(color=top20["ty_le_tieu_cuc"], colorscale=T.SEQ_RED, cmin=0, cmax=100,
                                colorbar=dict(title="% tiêu cực", thickness=10)),
                    text=[T.num(x) for x in top20["tong_so_bai"]], textposition="outside", cliponaxis=False,
                    hovertemplate="%{y}: <b>%{x:,}</b> bài<extra></extra>"))
                fig.update_layout(title="Top 20 thực thể được nhắc nhiều nhất", height=560,
                                  yaxis=dict(autorange="reversed", showgrid=False), xaxis=dict(showgrid=False, showticklabels=False))
                st.plotly_chart(fig, width="stretch")
            with c2:
                zmax = float(hot["z_score"].max()) if len(hot) else 1.0
                st.dataframe(
                    hot[["crawled_date", "entity", "so_bai", "tb_so_bai", "z_score", "ty_le_tieu_cuc", "muc_do_canh_bao"]].head(100),
                    width="stretch", hide_index=True, height=560,
                    column_config={
                        "crawled_date": st.column_config.DateColumn("Ngày", format="DD/MM/YYYY"),
                        "entity": "Thực thể",
                        "so_bai": st.column_config.NumberColumn("Số bài", format="%d"),
                        "tb_so_bai": st.column_config.NumberColumn("TB/ngày", format="%.1f"),
                        "z_score": st.column_config.ProgressColumn("Z-score", format="%.2f", min_value=0, max_value=zmax),
                        "ty_le_tieu_cuc": st.column_config.NumberColumn("% tiêu cực", format="%.0f%%"),
                        "muc_do_canh_bao": "Mức",
                    })
            pick = st.selectbox("Xem diễn biến theo ngày của thực thể:", top_ent["entity"].head(100).tolist())
            series = ent_alert[ent_alert["entity"] == pick].sort_values("crawled_date")
            fig = go.Figure()
            fig.add_trace(go.Bar(x=series["crawled_date"], y=series["so_bai"], name="Số bài",
                                 marker=dict(color=[T.NEGATIVE if z >= 2 else T.PRIMARY for z in series["z_score"]]),
                                 hovertemplate="%{x|%d/%m}: <b>%{y}</b> bài<extra></extra>"))
            fig.add_trace(go.Scatter(x=series["crawled_date"], y=series["z_score"], name="Z-score", yaxis="y2",
                                     mode="lines+markers", line=dict(color=T.INK, width=2, shape="spline")))
            fig.update_layout(title=f"Diễn biến của '{pick}' (cột đỏ: ngày đột biến Z ≥ 2)", height=380,
                              yaxis=dict(title="Số bài"), yaxis2=dict(title="Z-score", overlaying="y", side="right", showgrid=False),
                              hovermode="x unified", bargap=0.3, xaxis_tickformat="%d/%m")
            st.plotly_chart(fig, width="stretch")

    with tab6:
        render_pipeline_tab()

    T.footer()
