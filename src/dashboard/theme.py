"""
Giao diện Dashboard: bảng màu, template Plotly dùng chung, CSS và các khối HTML (header, thẻ KPI).
"""
import html

import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

# Bảng màu chính
PRIMARY = "#2563eb"
INK = "#0f172a"
MUTED = "#64748b"
GRID = "#e2e8f0"

POSITIVE = "#16a34a"
NEUTRAL = "#f59e0b"
NEGATIVE = "#dc2626"
SENTIMENT_COLORS = {"Tích cực": POSITIVE, "Trung tính": NEUTRAL, "Tiêu cực": NEGATIVE}

CATEGORICAL = ["#2563eb", "#0d9488", "#f59e0b", "#db2777", "#7c3aed", "#ea580c",
               "#0891b2", "#65a30d", "#e11d48", "#4f46e5", "#ca8a04", "#059669"]
SEQ_BLUE = ["#dbeafe", "#93c5fd", "#3b82f6", "#1d4ed8", "#1e3a8a"]
SEQ_TEAL = ["#ccfbf1", "#5eead4", "#14b8a6", "#0f766e", "#134e4a"]
SEQ_RED = ["#fee2e2", "#fca5a5", "#ef4444", "#b91c1c", "#7f1d1d"]


def register_plotly_template():
    """Template 'bd05' áp dụng cho mọi biểu đồ: nền trong suốt, lưới mảnh, font và màu thống nhất."""
    tpl = go.layout.Template(pio.templates["plotly_white"])
    tpl.layout.update(
        font=dict(family="Inter, Segoe UI, sans-serif", size=13, color=INK),
        title=dict(font=dict(size=15, color=INK), x=0, xanchor="left", pad=dict(b=8)),
        colorway=CATEGORICAL,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=50, b=10),
        hoverlabel=dict(bgcolor="white", bordercolor=GRID, font=dict(color=INK)),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title_text=""),
        xaxis=dict(gridcolor=GRID, linecolor=GRID, zeroline=False, title_font=dict(color=MUTED)),
        yaxis=dict(gridcolor=GRID, linecolor=GRID, zeroline=False, title_font=dict(color=MUTED)),
        coloraxis=dict(colorbar=dict(thickness=10, outlinewidth=0)),
    )
    pio.templates["bd05"] = tpl
    pio.templates.default = "bd05"


CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
html, body, [class*="css"], .stMarkdown, .stText { font-family: 'Inter', 'Segoe UI', sans-serif; }
.stApp { background: #f4f6fb; }
.block-container { padding-top: 2.2rem; padding-bottom: 2rem; max-width: 1500px; }
#MainMenu, footer, header [data-testid="stToolbar"] { visibility: hidden; }

/* Header */
.bd-hero {
  background: linear-gradient(120deg, #1e3a8a 0%, #2563eb 55%, #0ea5e9 100%);
  border-radius: 18px; padding: 26px 30px; color: #fff; margin-bottom: 18px;
  box-shadow: 0 10px 30px -12px rgba(37, 99, 235, .55);
}
.bd-hero h1 { color: #fff; font-size: 1.75rem; font-weight: 700; margin: 0 0 6px 0; padding: 0; letter-spacing: -.01em; }
.bd-hero p { color: rgba(255,255,255,.85); margin: 0; font-size: .95rem; }
.bd-chips { margin-top: 14px; display: flex; flex-wrap: wrap; gap: 8px; }
.bd-chip { background: rgba(255,255,255,.16); border: 1px solid rgba(255,255,255,.25);
  border-radius: 999px; padding: 3px 12px; font-size: .78rem; color: #fff; }

/* Thẻ KPI */
.bd-kpis { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 14px; margin-bottom: 8px; }
@media (max-width: 1100px) { .bd-kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
.bd-kpi { background: #fff; border: 1px solid #e6eaf2; border-radius: 16px; padding: 16px 18px 12px 18px;
  box-shadow: 0 1px 2px rgba(15,23,42,.04), 0 8px 24px -16px rgba(15,23,42,.25); position: relative; overflow: hidden; }
.bd-kpi::before { content: ""; position: absolute; left: 0; top: 0; right: 0; height: 4px; background: var(--accent); }
.bd-kpi .head { display: flex; justify-content: space-between; align-items: center; }
.bd-kpi .icon { width: 30px; height: 30px; border-radius: 9px; display: grid; place-items: center; font-size: .95rem;
  background: color-mix(in srgb, var(--accent) 14%, white); }
.bd-kpi .delta { font-size: .72rem; vertical-align: middle; font-weight: 600; padding: 2px 8px; border-radius: 99px; margin-left: 6px; }
.bd-kpi .delta.up { color: #15803d; background: #dcfce7; }
.bd-kpi .delta.down { color: #b91c1c; background: #fee2e2; }
.bd-kpi .spark { margin-top: 8px; display: block; }
.bd-kpi .label { color: #64748b; font-size: .76rem; font-weight: 600; text-transform: uppercase; letter-spacing: .03em;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.bd-kpi .value { color: #0f172a; font-size: 1.75rem; font-weight: 700; margin-top: 4px; line-height: 1.15; }
.bd-kpi .sub { color: #64748b; font-size: .82rem; margin-top: 2px; }
.bd-kpi .bar { height: 6px; border-radius: 99px; background: #f1f5f9; margin-top: 10px; overflow: hidden; }
.bd-kpi .bar span { display: block; height: 100%; background: var(--accent); border-radius: 99px; }

/* Thẻ điểm nóng */
.bd-hot { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; margin: 6px 0 4px 0; }
@media (max-width: 1100px) { .bd-hot { grid-template-columns: 1fr; } }
.bd-hot-card { background: #fff; border: 1px solid #e6eaf2; border-radius: 16px; padding: 14px 16px;
  box-shadow: 0 8px 24px -18px rgba(15,23,42,.35); display: flex; gap: 14px; align-items: center; }
.bd-hot-card .rank { min-width: 44px; height: 44px; border-radius: 12px; display: grid; place-items: center;
  font-weight: 700; color: #fff; background: var(--accent); font-size: 1.05rem; }
.bd-hot-card .title { font-weight: 650; color: #0f172a; font-size: .95rem; line-height: 1.3; }
.bd-hot-card .meta { color: #64748b; font-size: .8rem; margin-top: 3px; }
.bd-hot-card .z { margin-left: auto; text-align: right; min-width: 70px; }
.bd-hot-card .z b { display: block; font-size: 1.3rem; color: var(--accent); }
.bd-hot-card .z span { font-size: .72rem; color: #94a3b8; text-transform: uppercase; letter-spacing: .05em; }

/* Ô nhận định */
.bd-insight { background: linear-gradient(90deg, #eff6ff, #ffffff); border: 1px solid #dbeafe; border-left: 4px solid #2563eb;
  border-radius: 12px; padding: 10px 14px; color: #1e3a8a; font-size: .9rem; margin: 6px 0 10px 0; }

/* Thẻ chủ đề LDA */
.bd-topics { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }
@media (max-width: 1100px) { .bd-topics { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
.bd-topic { background: #fff; border: 1px solid #e6eaf2; border-radius: 14px; padding: 12px 14px; border-top: 4px solid var(--accent); }
.bd-topic .t-head { display: flex; justify-content: space-between; font-weight: 650; color: #0f172a; font-size: .9rem; }
.bd-topic .t-head span { color: #64748b; font-weight: 500; font-size: .8rem; }
.bd-topic .t-bar { height: 5px; background: #f1f5f9; border-radius: 99px; margin: 8px 0; overflow: hidden; }
.bd-topic .t-bar i { display: block; height: 100%; background: var(--accent); }
.bd-topic .kw { display: inline-block; background: #f1f5f9; color: #334155; border-radius: 99px; padding: 2px 9px;
  font-size: .76rem; margin: 2px 3px 2px 0; }

/* Thẻ chỉ số mô hình */
.bd-score { background: #fff; border: 1px solid #e6eaf2; border-radius: 16px; padding: 16px 18px; }
.bd-score .label { color: #64748b; font-size: .8rem; font-weight: 600; }
.bd-score .value { font-size: 2rem; font-weight: 700; color: #0f172a; }
.bd-score .ring { height: 8px; background: #f1f5f9; border-radius: 99px; overflow: hidden; margin-top: 6px; }
.bd-score .ring i { display: block; height: 100%; background: var(--accent); }
.bd-score .note { color: #64748b; font-size: .8rem; margin-top: 8px; }

/* Tiêu đề khu vực */
.bd-section { font-size: 1.05rem; font-weight: 650; color: #0f172a; margin: 18px 0 4px 0;
  display: flex; align-items: center; gap: 8px; }
.bd-section::before { content: ""; width: 4px; height: 18px; background: #2563eb; border-radius: 4px; }
.bd-note { color: #64748b; font-size: .85rem; margin-bottom: 8px; }

/* Tabs */
.stTabs [data-baseweb="tab-list"] { gap: 6px; background: #fff; padding: 6px; border-radius: 14px;
  border: 1px solid #e6eaf2; box-shadow: 0 8px 24px -20px rgba(15,23,42,.4); }
.stTabs [data-baseweb="tab"] { background: transparent; border-radius: 10px; padding: 8px 16px; font-weight: 600; height: auto; }
.stTabs [data-baseweb="tab"]:hover { background: #f1f5f9; }
.stTabs [data-baseweb="tab-border"] { display: none; }
.stTabs [aria-selected="true"] { background: #2563eb !important; color: #fff !important; box-shadow: 0 6px 16px -8px rgba(37,99,235,.8); }
.stTabs [aria-selected="true"] p { color: #fff !important; }
.stTabs [data-baseweb="tab-highlight"] { display: none; }

/* Biểu đồ, bảng, metric mặc định thành dạng thẻ */
[data-testid="stPlotlyChart"], [data-testid="stDataFrame"] {
  background: #fff; border: 1px solid #e6eaf2; border-radius: 16px; padding: 10px;
  box-shadow: 0 8px 24px -20px rgba(15,23,42,.35);
}
[data-testid="stMetric"] { background: #fff; border: 1px solid #e2e8f0; border-radius: 12px; padding: 12px 16px; }

/* Sidebar */
[data-testid="stSidebar"] { background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%); }
[data-testid="stSidebar"] * { color: #e2e8f0; }
[data-testid="stSidebar"] [data-baseweb="select"] > div,
[data-testid="stSidebar"] [data-baseweb="input"], [data-testid="stSidebar"] [data-baseweb="input"] > div {
  background: #fff !important; border-radius: 10px; }
[data-testid="stSidebar"] [data-testid="stDateInput"] > div > div,
[data-testid="stSidebar"] [data-testid="stDateInput"] > div > div * { background-color: #fff !important; }
[data-testid="stSidebar"] [data-testid="stDateInput"] > div > div { border-radius: 10px; overflow: hidden; }
[data-testid="stSidebar"] [data-testid="stDateInput"] > div > div * { color: #0f172a !important; -webkit-text-fill-color: #0f172a !important; }
[data-testid="stSidebar"] [data-baseweb="select"] *, [data-testid="stSidebar"] input {
  color: #0f172a !important; -webkit-text-fill-color: #0f172a !important; }
.bd-side-title { font-size: 1.15rem; font-weight: 700; color: #fff; margin-bottom: 2px; }
.bd-side-sub { font-size: .8rem; color: #94a3b8; margin-bottom: 14px; }
.bd-side-box { background: rgba(255,255,255,.06); border: 1px solid rgba(255,255,255,.1);
  border-radius: 12px; padding: 12px 14px; font-size: .82rem; color: #cbd5e1; margin-top: 18px; line-height: 1.6; }
</style>
"""


def inject_css():
    st.markdown(CSS, unsafe_allow_html=True)


def hero(title, subtitle, chips):
    chips_html = "".join(f'<span class="bd-chip">{html.escape(c)}</span>' for c in chips)
    st.markdown(
        f'<div class="bd-hero"><h1>{html.escape(title)}</h1><p>{html.escape(subtitle)}</p>'
        f'<div class="bd-chips">{chips_html}</div></div>',
        unsafe_allow_html=True,
    )


def sparkline(values, color, width=220, height=36):
    """Biểu đồ đường mini (SVG nội tuyến) cho thẻ KPI."""
    vals = [float(v) for v in values if v == v]
    if len(vals) < 2:
        return ""
    lo, hi = min(vals), max(vals)
    span = (hi - lo) or 1.0
    step = width / (len(vals) - 1)
    pts = [(i * step, height - 3 - (v - lo) / span * (height - 6)) for i, v in enumerate(vals)]
    line = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    area = f"0,{height} " + line + f" {width},{height}"
    return (f'<svg class="spark" viewBox="0 0 {width} {height}" preserveAspectRatio="none" width="100%" height="{height}">'
            f'<polygon points="{area}" fill="{color}" fill-opacity="0.12"/>'
            f'<polyline points="{line}" fill="none" stroke="{color}" stroke-width="2" stroke-linejoin="round"/></svg>')


def kpi_cards(cards):
    """cards: list dict(label, value, sub, accent, icon?, pct?, delta?, spark?)."""
    items = []
    for c in cards:
        accent = c.get("accent", PRIMARY)
        bar = ""
        if c.get("pct") is not None:
            bar = f'<div class="bar"><span style="width:{max(0.0, min(100.0, c["pct"])):.1f}%"></span></div>'
        delta = ""
        if c.get("delta") is not None:
            d = c["delta"]
            good = d >= 0 if not c.get("delta_inverse") else d <= 0
            delta = f'<span class="delta {"up" if good else "down"}">{"▲" if d >= 0 else "▼"} {abs(d):.1f}%</span>'
        spark = sparkline(c["spark"], accent) if c.get("spark") is not None else ""
        icon = f'<div class="icon">{html.escape(c["icon"])}</div>' if c.get("icon") else ""
        items.append(
            f'<div class="bd-kpi" style="--accent:{accent}">'
            f'<div class="head"><div class="label">{html.escape(c["label"])}</div>{icon}</div>'
            f'<div class="value">{html.escape(c["value"])}{delta}</div>'
            f'<div class="sub">{html.escape(c.get("sub", ""))}</div>{bar}{spark}</div>'
        )
    st.markdown(f'<div class="bd-kpis">{"".join(items)}</div>', unsafe_allow_html=True)


def hot_cards(items):
    """Thẻ 'điểm nóng': items = list dict(title, meta, z, accent)."""
    cards = []
    for i, it in enumerate(items, 1):
        cards.append(
            f'<div class="bd-hot-card" style="--accent:{it.get("accent", NEGATIVE)}">'
            f'<div class="rank">#{i}</div>'
            f'<div><div class="title">{html.escape(it["title"])}</div><div class="meta">{html.escape(it["meta"])}</div></div>'
            f'<div class="z"><b>{it["z"]:.2f}</b><span>Z-score</span></div></div>'
        )
    st.markdown(f'<div class="bd-hot">{"".join(cards)}</div>', unsafe_allow_html=True)


def insight(text):
    st.markdown(f'<div class="bd-insight">💡 {html.escape(text)}</div>', unsafe_allow_html=True)


def topic_cards(rows, max_count):
    """rows: list dict(id, count, keywords) cho mô hình chủ đề LDA."""
    cards = []
    for i, r in enumerate(rows):
        accent = CATEGORICAL[i % len(CATEGORICAL)]
        kws = "".join(f'<span class="kw">{html.escape(k)}</span>' for k in r["keywords"][:8])
        pct = r["count"] / max_count * 100 if max_count else 0
        cards.append(
            f'<div class="bd-topic" style="--accent:{accent}">'
            f'<div class="t-head">Chủ đề {r["id"]}<span>{num(r["count"])} bài</span></div>'
            f'<div class="t-bar"><i style="width:{pct:.1f}%"></i></div>{kws}</div>'
        )
    st.markdown(f'<div class="bd-topics">{"".join(cards)}</div>', unsafe_allow_html=True)


def score_card(label, value, note, accent):
    """Thẻ chỉ số 0–1 có thanh tỷ lệ (Accuracy, Macro-F1...)."""
    st.markdown(
        f'<div class="bd-score" style="--accent:{accent}"><div class="label">{html.escape(label)}</div>'
        f'<div class="value">{value:.3f}</div><div class="ring"><i style="width:{value * 100:.1f}%"></i></div>'
        f'<div class="note">{html.escape(note)}</div></div>',
        unsafe_allow_html=True,
    )


def num(x):
    """Số kiểu Việt Nam: 47.976"""
    return f"{int(x):,}".replace(",", ".")


def section(title, note=None):
    st.markdown(f'<div class="bd-section">{html.escape(title)}</div>', unsafe_allow_html=True)
    if note:
        st.markdown(f'<div class="bd-note">{html.escape(note)}</div>', unsafe_allow_html=True)
