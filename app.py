"""
Kenya Fuel Price Intelligence — Executive Dashboard
=====================================================
Streamlit + PostgreSQL | EPRA pump price data 2006–2026
"""

import os
import sys
from typing import List, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()

try:
    from streamlit.runtime.scriptrunner import get_script_run_ctx
    if get_script_run_ctx() is None and __name__ == "__main__":
        print("Run with:  streamlit run app.py")
        sys.exit(0)
except ImportError:
    pass


# ═══════════════════════════════════════════════════════════════
# PAGE CONFIG
# ═══════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Kenya Fuel Price Intelligence",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


# ═══════════════════════════════════════════════════════════════
# DESIGN TOKENS
# ═══════════════════════════════════════════════════════════════
BG_BASE  = "#0d1117"
BG_CARD  = "#161b22"
BG_CARD2 = "#1c2128"
BORDER   = "rgba(255,255,255,0.07)"
TEXT_PRI = "#e6edf3"
TEXT_SEC = "#8b949e"
TEXT_MUT = "#6e7681"
BLUE     = "#58a6ff"
GREEN    = "#3fb950"
AMBER    = "#d29922"
RED      = "#f85149"
PURPLE   = "#a371f7"

FUEL_COLORS = {"Petrol": BLUE, "Diesel": GREEN, "Kerosene": AMBER}
FUEL_COLUMNS = {"Petrol": "petrol_price", "Diesel": "diesel_price", "Kerosene": "kerosene_price"}
TABLE_NAME = "fuel_price_rows"


# ═══════════════════════════════════════════════════════════════
# GLOBAL CSS
# ═══════════════════════════════════════════════════════════════
st.markdown(f"""
<style>
html,body,[data-testid="stAppViewContainer"],[data-testid="stMain"],[data-testid="block-container"]{{
    background:{BG_BASE}!important;
}}
[data-testid="stHeader"]{{background:{BG_BASE}!important;border-bottom:0.5px solid {BORDER};}}
[data-testid="stSidebar"]{{background:{BG_CARD}!important;border-right:0.5px solid {BORDER};}}
[data-testid="stSidebar"] label,[data-testid="stSidebar"] p,[data-testid="stSidebar"] span{{color:{TEXT_SEC}!important;}}
[data-testid="stSidebar"] .stTextInput input{{background:{BG_BASE}!important;color:{TEXT_PRI}!important;border:0.5px solid {BORDER}!important;border-radius:8px!important;}}
h1,h2,h3,h4{{color:{TEXT_PRI}!important;font-weight:500!important;letter-spacing:-0.01em;}}
p,span,div,label{{color:{TEXT_SEC};}}
hr{{border-color:{BORDER}!important;margin:1.5rem 0!important;}}
[data-testid="stMetric"]{{background:{BG_CARD}!important;border:0.5px solid {BORDER};border-radius:12px!important;padding:1.1rem 1.3rem!important;}}
[data-testid="stMetricLabel"]{{font-size:11px!important;text-transform:uppercase;letter-spacing:.06em;color:{TEXT_MUT}!important;}}
[data-testid="stMetricValue"]{{font-size:1.7rem!important;font-weight:500!important;color:{TEXT_PRI}!important;}}
[data-testid="stMetricDelta"]{{font-size:0.82rem!important;}}
.kpi-card{{background:{BG_CARD};border:0.5px solid {BORDER};border-radius:12px;padding:18px 20px;height:100%;}}
.kpi-label{{font-size:11px;letter-spacing:.06em;color:{TEXT_MUT};text-transform:uppercase;margin-bottom:6px;}}
.kpi-value{{font-size:26px;font-weight:500;color:{TEXT_PRI};line-height:1.1;}}
.kpi-sub{{font-size:12px;margin-top:5px;}}
.kpi-up{{color:{RED};}} .kpi-down{{color:{GREEN};}} .kpi-neu{{color:{TEXT_MUT};}}
.section-title{{font-size:11px;font-weight:500;letter-spacing:.08em;text-transform:uppercase;color:{TEXT_MUT};
    border-left:2px solid {BLUE};padding-left:8px;margin:28px 0 14px;}}
.insight-box{{background:rgba(88,166,255,0.06);border:0.5px solid rgba(88,166,255,0.2);border-left:3px solid {BLUE};
    border-radius:8px;padding:10px 14px;margin-bottom:8px;font-size:13px;line-height:1.6;color:{TEXT_SEC};}}
.insight-box.green{{background:rgba(63,185,80,0.06);border-color:rgba(63,185,80,0.2);border-left-color:{GREEN};}}
.insight-box.amber{{background:rgba(210,153,34,0.06);border-color:rgba(210,153,34,0.2);border-left-color:{AMBER};}}
.insight-box.red{{background:rgba(248,81,73,0.06);border-color:rgba(248,81,73,0.2);border-left-color:{RED};}}
.insight-box.purple{{background:rgba(163,113,247,0.06);border-color:rgba(163,113,247,0.2);border-left-color:{PURPLE};}}
.stat-card{{background:{BG_CARD};border:0.5px solid {BORDER};border-radius:10px;padding:14px 16px;font-size:13px;line-height:2;}}
.stat-card b{{color:{BLUE};font-weight:500;}}
[data-testid="stTabs"] button{{color:{TEXT_MUT}!important;font-size:13px!important;font-weight:400!important;}}
[data-testid="stTabs"] button[aria-selected="true"]{{color:{TEXT_PRI}!important;font-weight:500!important;border-bottom:2px solid {BLUE}!important;}}
[data-testid="stDataFrame"]{{border-radius:10px;overflow:hidden;}}
.stButton>button{{background:{BG_CARD2}!important;color:{TEXT_PRI}!important;border:0.5px solid {BORDER}!important;border-radius:8px!important;font-size:13px!important;}}
.stButton>button:hover{{border-color:{BLUE}!important;}}
[data-testid="stExpander"]{{background:{BG_CARD}!important;border:0.5px solid {BORDER}!important;border-radius:10px!important;}}
[data-testid="stPlotlyChart"]{{background:{BG_CARD};border:0.5px solid {BORDER};border-radius:12px;padding:4px;}}
[data-testid="stDownloadButton"] button{{background:{BLUE}!important;color:#0d1117!important;border:none!important;border-radius:8px!important;font-weight:500!important;}}
footer{{visibility:hidden;}}
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
# PLOTLY DEFAULTS
# ═══════════════════════════════════════════════════════════════
def _fig(fig, height=460, title="", **kw):
    fig.update_layout(
        height=height,
        title=dict(text=title, font=dict(size=14, color=TEXT_PRI), x=0, xanchor="left"),
        template="plotly_dark",
        paper_bgcolor=BG_CARD, plot_bgcolor=BG_CARD,
        font=dict(family="Inter, sans-serif", size=12, color=TEXT_SEC),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
                    bgcolor="rgba(0,0,0,0)", font=dict(size=11, color=TEXT_SEC)),
        margin=dict(t=60, b=40, l=10, r=10),
        **kw,
    )
    fig.update_xaxes(gridcolor="rgba(255,255,255,0.04)", linecolor=BORDER,
                     tickfont=dict(color=TEXT_MUT), title_font=dict(color=TEXT_SEC))
    fig.update_yaxes(gridcolor="rgba(255,255,255,0.04)", linecolor=BORDER,
                     tickfont=dict(color=TEXT_MUT), title_font=dict(color=TEXT_SEC))
    return fig


def hex_rgba(h, a=0.15):
    r, g, b = int(h[1:3],16), int(h[3:5],16), int(h[5:7],16)
    return f"rgba({r},{g},{b},{a})"


# ═══════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════
def _build_url() -> str:
    try:
        if "postgres" in st.secrets:
            c = st.secrets["postgres"]
            return f"postgresql+psycopg2://{c['user']}:{c['password']}@{c['host']}:{c.get('port',5432)}/{c['dbname']}"
    except Exception:
        pass
    url = os.getenv("DATABASE_URL","")
    if url:
        url = url.replace("postgres://","postgresql+psycopg2://",1)
        if "+psycopg2" not in url:
            url = url.replace("postgresql://","postgresql+psycopg2://",1)
        return url
    u,pw = os.getenv("PGUSER","postgres"), os.getenv("PGPASSWORD","postgres")
    h,p,db = os.getenv("PGHOST","localhost"), os.getenv("PGPORT","5432"), os.getenv("PGDATABASE","postgres")
    return f"postgresql+psycopg2://{u}:{pw}@{h}:{p}/{db}"


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    return create_engine(_build_url(), pool_pre_ping=True)


@st.cache_data(ttl=600, show_spinner="Querying database…")
def load_raw(table: str) -> pd.DataFrame:
    q = text(f"""
        SELECT id, row_hash AS record_key, source AS source_name, document_url AS source_url,
               canonical_month, period_start::date AS price_date, period_end::date AS effective_end,
               town, county,
               super_petrol::numeric AS petrol_price,
               diesel::numeric       AS diesel_price,
               kerosene::numeric     AS kerosene_price,
               extraction_method AS price_type, data_quality,
               inserted_at AS loaded_at
        FROM {table}
        WHERE period_start IS NOT NULL
    """)
    with get_engine().connect() as conn:
        df = pd.read_sql(q, conn)
    df["price_date"] = pd.to_datetime(df["price_date"], errors="coerce")
    df["loaded_at"]  = pd.to_datetime(df["loaded_at"],  errors="coerce")
    for c in ["petrol_price","diesel_price","kerosene_price"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ["town","county","source_name","price_type","data_quality"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df


# ═══════════════════════════════════════════════════════════════
# TRANSFORMATIONS
# ═══════════════════════════════════════════════════════════════
def to_long(df: pd.DataFrame) -> pd.DataFrame:
    id_vars = ["id","record_key","source_name","source_url","price_date",
               "effective_end","town","county","price_type","data_quality","loaded_at"]
    long = df.melt(id_vars=id_vars, value_vars=list(FUEL_COLUMNS.values()),
                   var_name="fuel_col", value_name="price")
    rev = {v:k for k,v in FUEL_COLUMNS.items()}
    long["fuel"]       = long["fuel_col"].map(rev)
    long["year"]       = long["price_date"].dt.year
    long["month"]      = long["price_date"].dt.month
    long["year_month"] = long["price_date"].dt.to_period("M").astype(str)
    return long.dropna(subset=["price"])


def snap(df_long: pd.DataFrame, offset: int = 0) -> pd.DataFrame:
    dates = sorted(df_long["price_date"].dropna().unique())
    if len(dates) <= offset:
        return pd.DataFrame()
    return df_long[df_long["price_date"] == dates[-(offset+1)]].copy()


def national_trend(df_long: pd.DataFrame) -> pd.DataFrame:
    t = (df_long.groupby(["price_date","fuel"], as_index=False)["price"]
         .agg(avg="mean", lo="min", hi="max")
         .sort_values(["fuel","price_date"]))
    t["mom"]     = t.groupby("fuel")["avg"].diff()
    t["mom_pct"] = t.groupby("fuel")["avg"].pct_change() * 100
    return t


def county_snap(df_latest: pd.DataFrame) -> pd.DataFrame:
    return (df_latest.groupby(["county","fuel"], as_index=False)["price"]
            .mean().rename(columns={"price":"avg_price"})
            .sort_values(["fuel","avg_price"], ascending=[True,False]))


def volatility_stats(df_long: pd.DataFrame) -> pd.DataFrame:
    v = (df_long.groupby(["county","fuel"], as_index=False)["price"]
         .agg(avg="mean", std="std", lo="min", hi="max"))
    v["spread"] = v["hi"] - v["lo"]
    v["cv"]     = np.where(v["avg"] > 0, v["std"] / v["avg"] * 100, np.nan)
    return v


def town_trend_df(df_long: pd.DataFrame, towns: List[str]) -> pd.DataFrame:
    if not towns:
        return pd.DataFrame()
    return (df_long[df_long["town"].isin(towns)]
            .groupby(["price_date","town","fuel"], as_index=False)["price"].mean())


def yoy_change(df_long: pd.DataFrame) -> pd.DataFrame:
    annual = (df_long.groupby(["year","fuel"], as_index=False)["price"].mean()
              .sort_values(["fuel","year"]))
    annual["yoy"]     = annual.groupby("fuel")["price"].diff()
    annual["yoy_pct"] = annual.groupby("fuel")["price"].pct_change() * 100
    return annual.dropna(subset=["yoy"])


def heatmap_data(df_long: pd.DataFrame, fuel: str) -> pd.DataFrame:
    sub = df_long[df_long["fuel"] == fuel]
    return (sub.groupby(["county","year_month"], as_index=False)["price"].mean()
            .pivot(index="county", columns="year_month", values="price")
            .sort_index())


def cross_fuel_corr(df_latest: pd.DataFrame) -> pd.DataFrame:
    return (df_latest.pivot_table(index=["town","county"],
                                   columns="fuel", values="price", aggfunc="mean")
            .reset_index())


# ═══════════════════════════════════════════════════════════════
# INSIGHTS ENGINE — all three products
# ═══════════════════════════════════════════════════════════════
def generate_insights(df_long, df_latest, df_prev) -> dict:
    out = {k: [] for k in ["petrol","diesel","kerosene","cross","welfare","geo"]}

    for fuel in ["Petrol","Diesel","Kerosene"]:
        fkey   = fuel.lower()
        subset = df_long[df_long["fuel"] == fuel]
        if subset.empty:
            continue

        first_p = subset.sort_values("price_date").groupby("county")["price"].first().mean()
        last_p  = subset.sort_values("price_date").groupby("county")["price"].last().mean()
        chg     = last_p - first_p
        pct     = chg / first_p * 100 if first_p else 0
        direction = "risen" if chg >= 0 else "fallen"
        out[fkey].append(
            f"{fuel} has <b>{direction} {abs(pct):.1f}%</b> over the period "
            f"(KES {first_p:.2f} → KES {last_p:.2f}, net {chg:+.2f})."
        )

        if not df_prev.empty:
            cur  = df_latest[df_latest["fuel"]==fuel]["price"].mean()
            prev = df_prev[df_prev["fuel"]==fuel]["price"].mean()
            if pd.notna(prev) and prev > 0:
                mom = (cur - prev) / prev * 100
                arrow = "▲" if mom > 0 else "▼"
                out[fkey].append(
                    f"Month-on-month: <b>{arrow} {abs(mom):.1f}%</b> "
                    f"(KES {prev:.2f} → KES {cur:.2f})."
                )

        lat = df_latest[df_latest["fuel"]==fuel]
        if not lat.empty:
            hi_row = lat.loc[lat["price"].idxmax()]
            lo_row = lat.loc[lat["price"].idxmin()]
            spread = hi_row["price"] - lo_row["price"]
            out[fkey].append(
                f"Latest spread: <b>KES {spread:.2f}</b> — highest <b>{hi_row['town']}</b> "
                f"({hi_row['county']}) KES {hi_row['price']:.2f}, lowest <b>{lo_row['town']}</b> "
                f"({lo_row['county']}) KES {lo_row['price']:.2f}."
            )
            out["geo"].append(
                f"{fuel}: highest <b>{hi_row['town']}, {hi_row['county']}</b> "
                f"(KES {hi_row['price']:.2f}) · lowest <b>{lo_row['town']}, "
                f"{lo_row['county']}</b> (KES {lo_row['price']:.2f})"
            )

        vol_s = volatility_stats(subset)
        if not vol_s.empty:
            mv = vol_s.sort_values("cv", ascending=False).iloc[0]
            out[fkey].append(
                f"Most volatile county: <b>{mv['county']}</b> "
                f"(CV {mv['cv']:.1f}%, spread KES {mv['spread']:.2f})."
            )

        annual = subset.groupby("year")["price"].mean()
        if len(annual) > 1:
            pk = annual.idxmax()
            out[fkey].append(
                f"Peak annual avg: <b>{pk}</b> at KES {annual[pk]:.2f}/litre."
            )

    avgs = df_latest.groupby("fuel")["price"].mean()
    if "Petrol" in avgs and "Diesel" in avgs:
        ratio = avgs["Diesel"] / avgs["Petrol"] * 100
        out["cross"].append(
            f"Diesel/petrol ratio: <b>{ratio:.1f}%</b> — "
            f"{'converging toward parity (tax restructuring signal)' if ratio>88 else 'normal discount structure maintained'}."
        )
    if "Kerosene" in avgs and "Petrol" in avgs:
        kratio = avgs["Kerosene"] / avgs["Petrol"] * 100
        risk = "HIGH" if kratio > 75 else "MODERATE" if kratio > 60 else "LOW"
        out["cross"].append(
            f"Kerosene/petrol ratio: <b>{kratio:.1f}%</b> — "
            f"low-income energy burden risk is <b>{risk}</b>."
        )
        out["welfare"].append(
            f"Kerosene at <b>{kratio:.1f}%</b> of petrol. "
            f"{'Ratio exceeds 75% — subsidy review warranted.' if kratio>75 else 'Within historical norms.'}"
        )

    remote = ["Mandera","Wajir","Turkana","Marsabit","Lokichogio"]
    urban  = ["Nairobi","Mombasa","Kisumu","Nakuru","Eldoret"]
    for fuel in ["Petrol","Diesel","Kerosene"]:
        lat = df_latest[df_latest["fuel"]==fuel]
        r_avg = lat[lat["county"].isin(remote)]["price"].mean()
        u_avg = lat[lat["county"].isin(urban)]["price"].mean()
        if pd.notna(r_avg) and pd.notna(u_avg) and u_avg > 0:
            prem = (r_avg - u_avg) / u_avg * 100
            out["geo"].append(
                f"{fuel} remote premium: <b>+{prem:.1f}%</b> "
                f"(KES {r_avg:.2f} vs KES {u_avg:.2f})."
            )

    return out


# ═══════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown(f"<h2 style='color:{TEXT_PRI};margin-bottom:0'>Fuel Intelligence</h2>", unsafe_allow_html=True)
    st.markdown(f"<p style='color:{TEXT_MUT};font-size:12px;margin-top:4px'>Kenya EPRA Price Dashboard</p>", unsafe_allow_html=True)
    st.markdown("---")
    table = st.text_input("PostgreSQL table", value=TABLE_NAME)
    c_r1, c_r2 = st.columns(2)
    with c_r1:
        if st.button("Reload", use_container_width=True):
            load_raw.clear(); st.rerun()
    with c_r2:
        if st.button("Clear", use_container_width=True):
            st.cache_data.clear(); st.cache_resource.clear(); st.rerun()
    st.markdown("---")
    st.markdown(f"<p style='color:{TEXT_MUT};font-size:11px;text-transform:uppercase;letter-spacing:.06em'>Filters</p>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════
# LOAD
# ═══════════════════════════════════════════════════════════════
try:
    df_raw = load_raw(table)
except Exception as e:
    st.error("**Database connection failed.**")
    with st.expander("Error details"):
        st.exception(e)
    st.info("Set `DATABASE_URL=postgresql://user:password@host:5432/dbname` in your `.env` file.")
    st.stop()

if df_raw.empty:
    st.warning("Query returned no rows.")
    st.stop()

df_long = to_long(df_raw)

with st.sidebar:
    min_d = df_long["price_date"].min().date()
    max_d = df_long["price_date"].max().date()
    dr = st.date_input("Date range", value=(min_d, max_d), min_value=min_d, max_value=max_d)
    start_d, end_d = (dr[0], dr[1]) if isinstance(dr, tuple) and len(dr)==2 else (min_d, max_d)

    all_counties = sorted(df_long["county"].dropna().unique())
    sel_counties = st.multiselect("Counties", all_counties, default=[], placeholder="All counties")

    all_towns = sorted(df_long["town"].dropna().unique())
    default_bench = [t for t in ["Nairobi","Mombasa","Kisumu","Eldoret","Mandera"] if t in all_towns]
    bench_towns = st.multiselect("Benchmark towns", all_towns, default=default_bench)

    sel_fuels = st.multiselect("Fuel types", list(FUEL_COLUMNS.keys()), default=list(FUEL_COLUMNS.keys()))
    st.markdown("---")
    st.caption("Data: EPRA gazette extracts & market estimates")

# ── Apply filters ──
mask = (
    (df_long["price_date"].dt.date >= start_d) &
    (df_long["price_date"].dt.date <= end_d) &
    (df_long["fuel"].isin(sel_fuels))
)
if sel_counties:
    mask &= df_long["county"].isin(sel_counties)

flt = df_long[mask].copy()
if flt.empty:
    st.warning("No data matches the selected filters.")
    st.stop()

# Pre-compute
df_latest = snap(flt, 0)
df_prev   = snap(flt, 1)
nat_trnd  = national_trend(flt)
twn_trnd  = town_trend_df(flt, bench_towns)
cty_snp   = county_snap(df_latest)
vol       = volatility_stats(flt)
yoy       = yoy_change(flt)
insights  = generate_insights(flt, df_latest, df_prev)
latest_dt = pd.to_datetime(df_latest["price_date"].max())


# ═══════════════════════════════════════════════════════════════
# PAGE HEADER
# ═══════════════════════════════════════════════════════════════
st.markdown(
    f"<h1 style='margin-bottom:2px;color:{TEXT_PRI}'>Kenya Fuel Price Intelligence</h1>"
    f"<p style='color:{TEXT_MUT};margin:0;font-size:13px'>"
    f"EPRA pump prices · {start_d.strftime('%b %Y')} – {end_d.strftime('%b %Y')} · "
    f"{flt['county'].nunique()} counties · {flt['town'].nunique()} towns · {len(flt):,} records</p>",
    unsafe_allow_html=True,
)
st.markdown("---")


# ═══════════════════════════════════════════════════════════════
# KPI STRIP
# ═══════════════════════════════════════════════════════════════
def metric_delta(fuel):
    cur  = df_latest[df_latest["fuel"]==fuel]["price"].mean()
    prev = df_prev[df_prev["fuel"]==fuel]["price"].mean() if not df_prev.empty else np.nan
    dlt  = cur - prev if pd.notna(prev) else None
    return cur, dlt

c1,c2,c3,c4,c5,c6,c7 = st.columns(7)
for col, fuel in zip([c1,c2,c3], ["Petrol","Diesel","Kerosene"]):
    avg, dlt = metric_delta(fuel)
    with col:
        st.metric(f"Avg {fuel}", f"KES {avg:,.2f}" if pd.notna(avg) else "N/A",
                  delta=f"{dlt:+.2f}" if dlt is not None else None)

spread_avg = df_latest.groupby("fuel")["price"].agg(lambda x: x.max()-x.min()).mean()
with c4:
    st.metric("Avg Spread", f"KES {spread_avg:,.2f}" if pd.notna(spread_avg) else "N/A",
              help="Mean (max−min) across fuel types at latest snapshot.")

avgs_lat = df_latest.groupby("fuel")["price"].mean()
if "Diesel" in avgs_lat and "Petrol" in avgs_lat:
    with c5:
        st.metric("Diesel/Petrol", f"{avgs_lat['Diesel']/avgs_lat['Petrol']*100:.1f}%")
if "Kerosene" in avgs_lat and "Petrol" in avgs_lat:
    with c6:
        st.metric("Kerosene/Petrol", f"{avgs_lat['Kerosene']/avgs_lat['Petrol']*100:.1f}%",
                  help="Welfare proxy.")
with c7:
    st.metric("Latest Period", latest_dt.strftime("%b %Y"))

st.markdown("---")


# ═══════════════════════════════════════════════════════════════
# EXECUTIVE SUMMARY
# ═══════════════════════════════════════════════════════════════
st.markdown('<div class="section-title">Executive summary</div>', unsafe_allow_html=True)
sum_l, sum_r = st.columns([1.6, 1])

with sum_l:
    meta = {
        "petrol":  ("Petrol insights",   ""),
        "diesel":  ("Diesel insights",   "green"),
        "kerosene":("Kerosene insights", "amber"),
        "cross":   ("Cross-fuel",        "purple"),
        "welfare": ("Welfare signal",    "red"),
        "geo":     ("Geographic",        ""),
    }
    for key, (label, css) in meta.items():
        items = insights.get(key, [])
        if not items:
            continue
        st.markdown(f"<p style='font-size:11px;font-weight:500;color:{TEXT_MUT};margin:14px 0 4px;"
                    f"text-transform:uppercase;letter-spacing:.05em'>{label}</p>", unsafe_allow_html=True)
        for item in items:
            st.markdown(f"<div class='insight-box {css}'>{item}</div>", unsafe_allow_html=True)

with sum_r:
    qlt = df_raw["data_quality"].value_counts().to_dict()
    qlt_str = " · ".join([f"{k}: {v:,}" for k,v in qlt.items()])
    st.markdown(f"""
    <div class='stat-card'>
        <b>Coverage</b><br>
        Period: {start_d.strftime('%b %Y')} – {end_d.strftime('%b %Y')}<br>
        Snapshots: {flt['price_date'].nunique():,}<br>
        Towns: {flt['town'].nunique():,} | Counties: {flt['county'].nunique():,}<br>
        Records: {len(flt):,}<br>
        Quality: {qlt_str}
    </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    rows = []
    for fuel in sel_fuels:
        cur  = df_latest[df_latest["fuel"]==fuel]["price"].mean()
        prv  = df_prev[df_prev["fuel"]==fuel]["price"].mean() if not df_prev.empty else np.nan
        chg  = cur - prv if pd.notna(prv) else np.nan
        pct  = chg / prv * 100 if pd.notna(prv) and prv > 0 else np.nan
        rows.append({"Fuel": fuel,
                     "Latest": f"KES {cur:.2f}",
                     "Prev":   f"KES {prv:.2f}" if pd.notna(prv) else "—",
                     "Δ KES":  f"{chg:+.2f}" if pd.notna(chg) else "—",
                     "Δ %":    f"{pct:+.1f}%" if pd.notna(pct) else "—"})
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

st.markdown("---")


# ═══════════════════════════════════════════════════════════════
# TABS
# ═══════════════════════════════════════════════════════════════
tab1,tab2,tab3,tab4,tab5,tab6 = st.tabs([
    "Price Trends",
    "Regional",
    "Volatility",
    "Rankings",
    "Cross-fuel",
    "Raw Data",
])


# ───────────────────────────────────────────────────────────────
# TAB 1 — PRICE TRENDS
# ───────────────────────────────────────────────────────────────
with tab1:
    st.markdown('<div class="section-title">National average price corridor — all fuels</div>', unsafe_allow_html=True)
    fig_c = go.Figure()
    for fuel in sel_fuels:
        color = FUEL_COLORS[fuel]
        sub = nat_trnd[nat_trnd["fuel"]==fuel].sort_values("price_date")
        if sub.empty:
            continue
        fig_c.add_trace(go.Scatter(
            x=pd.concat([sub["price_date"], sub["price_date"][::-1]]),
            y=pd.concat([sub["hi"], sub["lo"][::-1]]),
            fill="toself", fillcolor=hex_rgba(color, 0.10),
            line=dict(width=0), showlegend=False, hoverinfo="skip",
        ))
        fig_c.add_trace(go.Scatter(
            x=sub["price_date"], y=sub["avg"].round(2),
            mode="lines+markers", name=fuel,
            line=dict(color=color, width=2.5), marker=dict(size=4),
            hovertemplate=f"<b>{fuel}</b> %{{x|%b %Y}}<br>Avg KES %{{y:.2f}}<extra></extra>",
        ))
    _fig(fig_c, height=480, title="National Average Price with Min–Max Corridor")
    fig_c.update_yaxes(title_text="KES / litre")
    st.plotly_chart(fig_c, use_container_width=True)

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown('<div class="section-title">Year-on-year % change</div>', unsafe_allow_html=True)
        fig_yoy = go.Figure()
        for fuel in sel_fuels:
            sub = yoy[yoy["fuel"]==fuel].sort_values("year")
            fig_yoy.add_trace(go.Bar(x=sub["year"], y=sub["yoy_pct"].round(1),
                                     name=fuel, marker_color=FUEL_COLORS[fuel],
                                     hovertemplate=f"<b>{fuel}</b> %{{x}}: %{{y:+.1f}}%<extra></extra>"))
        fig_yoy.add_hline(y=0, line_width=0.8, line_color="rgba(255,255,255,0.2)")
        _fig(fig_yoy, height=380, title="Year-on-Year Price Change (%)")
        fig_yoy.update_layout(barmode="group")
        fig_yoy.update_yaxes(title_text="YoY %")
        st.plotly_chart(fig_yoy, use_container_width=True)

    with col_b:
        st.markdown('<div class="section-title">Month-on-month change — last 12 periods</div>', unsafe_allow_html=True)
        mom12 = nat_trnd.sort_values(["fuel","price_date"]).groupby("fuel").tail(12)
        fig_mom = px.bar(mom12, x="price_date", y="mom", color="fuel",
                         color_discrete_map=FUEL_COLORS, barmode="group",
                         labels={"mom":"MoM Δ (KES)","price_date":"Period","fuel":"Fuel"},
                         text_auto=".1f")
        _fig(fig_mom, height=380, title="Month-on-Month Change — Last 12 Periods")
        fig_mom.update_traces(textfont_size=9, textposition="outside")
        fig_mom.add_hline(y=0, line_width=0.8, line_color="rgba(255,255,255,0.2)")
        st.plotly_chart(fig_mom, use_container_width=True)

    if not twn_trnd.empty:
        st.markdown('<div class="section-title">Benchmark town price comparison</div>', unsafe_allow_html=True)
        fig_t = px.line(twn_trnd, x="price_date", y="price",
                        color="town", line_dash="fuel",
                        labels={"price_date":"Period","price":"KES / litre","town":"Town","fuel":"Fuel"},
                        hover_data={"price":":.2f"})
        _fig(fig_t, height=460, title="Price Trend — Benchmark Towns by Fuel Type")
        fig_t.update_yaxes(title_text="KES / litre")
        st.plotly_chart(fig_t, use_container_width=True)
    else:
        st.info("Select benchmark towns in the sidebar to enable this chart.")


# ───────────────────────────────────────────────────────────────
# TAB 2 — REGIONAL
# ───────────────────────────────────────────────────────────────
with tab2:
    fuel_r2 = st.selectbox("Focus fuel", sel_fuels, key="tab2_fuel")
    nat_avg_r = df_latest[df_latest["fuel"]==fuel_r2]["price"].mean()

    left_r, right_r = st.columns([1.3, 1])
    with left_r:
        st.markdown('<div class="section-title">Latest average price by county</div>', unsafe_allow_html=True)
        cview = cty_snp[cty_snp["fuel"]==fuel_r2].sort_values("avg_price", ascending=True)
        fig_cb = px.bar(cview, x="avg_price", y="county", orientation="h",
                        color="avg_price",
                        color_continuous_scale=["#1f6feb","#d29922","#f85149"],
                        labels={"avg_price":"KES / litre","county":""},
                        text="avg_price", text_auto=".2f")
        _fig(fig_cb, height=max(500, len(cview)*20), title=f"Latest {fuel_r2} Price by County")
        fig_cb.update_coloraxes(showscale=False)
        fig_cb.update_traces(textposition="outside", textfont_size=9)
        st.plotly_chart(fig_cb, use_container_width=True)

    with right_r:
        st.markdown('<div class="section-title">Deviation from national average</div>', unsafe_allow_html=True)
        dev = cty_snp[cty_snp["fuel"]==fuel_r2].copy()
        dev["deviation"] = (dev["avg_price"] - nat_avg_r).round(2)
        dev = dev.sort_values("deviation")
        fig_dv = px.bar(dev, x="deviation", y="county", orientation="h",
                        color="deviation",
                        color_continuous_scale=[GREEN,"#1c2128",RED],
                        color_continuous_midpoint=0,
                        labels={"deviation":"Δ from national avg (KES)","county":""},
                        text="deviation", text_auto=".2f")
        _fig(fig_dv, height=max(500, len(dev)*20), title=f"{fuel_r2} — County vs National Average")
        fig_dv.update_coloraxes(showscale=False)
        fig_dv.update_traces(textposition="outside", textfont_size=9)
        fig_dv.add_vline(x=0, line_dash="dot", line_color="rgba(255,255,255,0.2)", line_width=1)
        st.plotly_chart(fig_dv, use_container_width=True)

    # Top/bottom 10 towns
    fuel_latest_r = df_latest[df_latest["fuel"]==fuel_r2]
    town_agg = fuel_latest_r.groupby(["town","county"], as_index=False)["price"].mean()
    top10 = town_agg.sort_values("price", ascending=False).head(10).copy()
    bot10 = town_agg.sort_values("price", ascending=True).head(10).copy()
    for d in [top10, bot10]:
        d["price"] = d["price"].round(2)
        d.insert(0,"#", range(1, len(d)+1))

    r_t1, r_t2 = st.columns(2)
    with r_t1:
        st.markdown(f"<p style='color:{RED};font-size:13px;font-weight:500'>Most expensive — {fuel_r2}</p>", unsafe_allow_html=True)
        st.dataframe(top10.rename(columns={"town":"Town","county":"County","price":"KES"}),
                     use_container_width=True, hide_index=True)
    with r_t2:
        st.markdown(f"<p style='color:{GREEN};font-size:13px;font-weight:500'>Most affordable — {fuel_r2}</p>", unsafe_allow_html=True)
        st.dataframe(bot10.rename(columns={"town":"Town","county":"County","price":"KES"}),
                     use_container_width=True, hide_index=True)

    st.markdown('<div class="section-title">Price heatmap — county × month</div>', unsafe_allow_html=True)
    heat = heatmap_data(flt, fuel_r2)
    if not heat.empty:
        fig_heat = go.Figure(go.Heatmap(
            z=heat.values, x=heat.columns.tolist(), y=heat.index.tolist(),
            colorscale="Blues",
            colorbar=dict(title=dict(text="KES", font=dict(color=TEXT_SEC)),
                          tickfont=dict(color=TEXT_MUT)),
            hovertemplate="County: %{y}<br>Month: %{x}<br>KES %{z:.2f}<extra></extra>",
        ))
        _fig(fig_heat, height=max(600, len(heat)*18), title=f"{fuel_r2} — Price Heatmap (County × Month)")
        fig_heat.update_xaxes(tickangle=-45)
        st.plotly_chart(fig_heat, use_container_width=True)

    st.markdown('<div class="section-title">All-fuel price mix by county — latest</div>', unsafe_allow_html=True)
    mix = df_latest.groupby(["county","fuel"], as_index=False)["price"].mean()
    fig_mix = px.bar(mix, x="county", y="price", color="fuel",
                     color_discrete_map=FUEL_COLORS, barmode="group",
                     labels={"price":"KES / litre","county":"County","fuel":"Fuel"},
                     text_auto=".0f")
    _fig(fig_mix, height=480, title="Latest Fuel Price Mix by County — All Products")
    fig_mix.update_layout(xaxis_tickangle=-45)
    fig_mix.update_traces(textposition="outside", textfont_size=8)
    st.plotly_chart(fig_mix, use_container_width=True)


# ───────────────────────────────────────────────────────────────
# TAB 3 — VOLATILITY
# ───────────────────────────────────────────────────────────────
with tab3:
    st.markdown('<div class="section-title">Price distribution — all fuel types</div>', unsafe_allow_html=True)
    v1, v2 = st.columns(2)
    with v1:
        fig_box = px.box(flt, x="fuel", y="price", color="fuel",
                         color_discrete_map=FUEL_COLORS, points="outliers", notched=True,
                         labels={"price":"KES / litre","fuel":"Fuel"})
        _fig(fig_box, height=440, title="Price Distribution by Fuel Type")
        fig_box.update_traces(marker_size=3)
        st.plotly_chart(fig_box, use_container_width=True)
    with v2:
        fig_vio = px.violin(flt, x="fuel", y="price", color="fuel",
                            color_discrete_map=FUEL_COLORS, box=True, points=False,
                            labels={"price":"KES / litre","fuel":"Fuel"})
        _fig(fig_vio, height=440, title="Violin — Price Spread Distribution")
        st.plotly_chart(fig_vio, use_container_width=True)

    st.markdown('<div class="section-title">County price volatility — coefficient of variation</div>', unsafe_allow_html=True)
    fuel_v = st.selectbox("Fuel", sel_fuels, key="vol_fuel")
    vol_v  = vol[vol["fuel"]==fuel_v].sort_values("cv", ascending=False).head(25)
    v3, v4 = st.columns([1.5, 1])
    with v3:
        fig_vol = px.bar(vol_v, x="cv", y="county", orientation="h",
                         color="cv", color_continuous_scale="Reds",
                         labels={"cv":"CV (%)","county":"County"},
                         text="cv", text_auto=".1f")
        _fig(fig_vol, height=600, title=f"Top 25 Counties — {fuel_v} Price Volatility")
        fig_vol.update_coloraxes(showscale=False)
        fig_vol.update_traces(textposition="outside", textfont_size=9)
        st.plotly_chart(fig_vol, use_container_width=True)
    with v4:
        st.dataframe(
            vol_v[["county","avg","lo","hi","spread","std","cv"]]
            .rename(columns={"county":"County","avg":"Avg","lo":"Min","hi":"Max","spread":"Spread","std":"Std","cv":"CV %"})
            .round(2),
            use_container_width=True, hide_index=True
        )

    st.markdown('<div class="section-title">Rolling 3-period price variability</div>', unsafe_allow_html=True)
    roll = (nat_trnd.copy().sort_values(["fuel","price_date"])
            .assign(roll_std=lambda d: d.groupby("fuel")["avg"].transform(lambda x: x.rolling(3).std())))
    fig_roll = px.line(roll, x="price_date", y="roll_std", color="fuel",
                       color_discrete_map=FUEL_COLORS,
                       labels={"price_date":"Period","roll_std":"3-Period Rolling Std Dev (KES)","fuel":"Fuel"})
    _fig(fig_roll, height=360, title="Rolling 3-Period Standard Deviation")
    fig_roll.update_yaxes(title_text="Std Dev (KES)")
    st.plotly_chart(fig_roll, use_container_width=True)


# ───────────────────────────────────────────────────────────────
# TAB 4 — RANKINGS
# ───────────────────────────────────────────────────────────────
with tab4:
    fuel_rk = st.selectbox("Fuel type", sel_fuels, key="rank_fuel")
    rk_data = (df_latest[df_latest["fuel"]==fuel_rk]
               .groupby(["town","county"], as_index=False)["price"].mean())
    top15 = rk_data.sort_values("price", ascending=False).head(15).copy()
    bot15 = rk_data.sort_values("price", ascending=True).head(15).copy()
    for d in [top15, bot15]:
        d["price"] = d["price"].round(2)
        d.insert(0,"Rank", range(1, len(d)+1))

    rk1, rk2 = st.columns(2)
    with rk1:
        st.markdown(f"<p style='color:{RED};font-weight:500;font-size:13px'>Most Expensive — {fuel_rk}</p>", unsafe_allow_html=True)
        st.dataframe(top15.rename(columns={"town":"Town","county":"County","price":"KES"}),
                     use_container_width=True, hide_index=True)
    with rk2:
        st.markdown(f"<p style='color:{GREEN};font-weight:500;font-size:13px'>Most Affordable — {fuel_rk}</p>", unsafe_allow_html=True)
        st.dataframe(bot15.rename(columns={"town":"Town","county":"County","price":"KES"}),
                     use_container_width=True, hide_index=True)

    st.markdown('<div class="section-title">National price band — full period</div>', unsafe_allow_html=True)
    band = (flt[flt["fuel"]==fuel_rk].groupby("price_date", as_index=False)["price"]
            .agg(avg="mean", lo="min", hi="max").sort_values("price_date"))
    color = FUEL_COLORS.get(fuel_rk, BLUE)
    fig_band = go.Figure()
    fig_band.add_trace(go.Scatter(
        x=pd.concat([band["price_date"], band["price_date"][::-1]]),
        y=pd.concat([band["hi"], band["lo"][::-1]]),
        fill="toself", fillcolor=hex_rgba(color, 0.12),
        line=dict(width=0), showlegend=False, hoverinfo="skip",
    ))
    fig_band.add_trace(go.Scatter(
        x=band["price_date"], y=band["avg"].round(2),
        mode="lines+markers", name=f"{fuel_rk} avg",
        line=dict(color=color, width=2.5), marker=dict(size=5),
        hovertemplate="<b>%{x|%b %Y}</b><br>Avg KES %{y:.2f}<extra></extra>",
    ))
    _fig(fig_band, height=420, title=f"{fuel_rk} — National Price Band (Min / Avg / Max)")
    fig_band.update_yaxes(title_text="KES / litre")
    st.plotly_chart(fig_band, use_container_width=True)

    st.markdown('<div class="section-title">All-county divergence from national average</div>', unsafe_allow_html=True)
    nat_avg_rk = df_latest[df_latest["fuel"]==fuel_rk]["price"].mean()
    div = cty_snp[cty_snp["fuel"]==fuel_rk].copy()
    div["vs_nat"] = (div["avg_price"] - nat_avg_rk).round(2)
    div = div.sort_values("vs_nat")
    fig_dg = px.bar(div, x="vs_nat", y="county", orientation="h",
                    color="vs_nat",
                    color_continuous_scale=[GREEN,"#1c2128",RED],
                    color_continuous_midpoint=0,
                    labels={"vs_nat":"Deviation (KES)","county":"County"},
                    text="vs_nat", text_auto=".2f")
    _fig(fig_dg, height=max(700, len(div)*18), title=f"{fuel_rk} — County Price vs National Average")
    fig_dg.update_coloraxes(showscale=False)
    fig_dg.update_traces(textposition="outside", textfont_size=9)
    fig_dg.add_vline(x=0, line_dash="dot", line_color="rgba(255,255,255,0.2)", line_width=1)
    st.plotly_chart(fig_dg, use_container_width=True)


# ───────────────────────────────────────────────────────────────
# TAB 5 — CROSS-FUEL
# ───────────────────────────────────────────────────────────────
with tab5:
    st.markdown('<div class="section-title">Diesel & kerosene as % of petrol price</div>', unsafe_allow_html=True)
    pivot = nat_trnd.pivot_table(index="price_date", columns="fuel", values="avg").reset_index()
    if "Petrol" in pivot.columns:
        for f in ["Diesel","Kerosene"]:
            if f in pivot.columns:
                pivot[f"{f}/Petrol %"] = (pivot[f] / pivot["Petrol"] * 100).round(1)

    rcols = {"Diesel/Petrol %": GREEN, "Kerosene/Petrol %": AMBER}
    fig_ratio = go.Figure()
    for rc, rc_color in rcols.items():
        if rc in pivot.columns:
            fig_ratio.add_trace(go.Scatter(
                x=pivot["price_date"], y=pivot[rc], name=rc, mode="lines",
                line=dict(color=rc_color, width=2.2),
                fill="tozeroy", fillcolor=hex_rgba(rc_color, 0.07),
                hovertemplate=f"<b>{rc}</b> %{{x|%b %Y}}: %{{y:.1f}}%<extra></extra>",
            ))
    fig_ratio.add_hline(y=100, line_dash="dot", line_color="rgba(255,255,255,0.15)", line_width=1)
    _fig(fig_ratio, height=400, title="Diesel & Kerosene as % of Petrol Price — Over Time")
    fig_ratio.update_yaxes(title_text="% of Petrol Price", range=[20,110])
    st.plotly_chart(fig_ratio, use_container_width=True)

    # Kerosene welfare index
    if "Kerosene/Petrol %" in pivot.columns:
        st.markdown('<div class="section-title">Kerosene welfare index</div>', unsafe_allow_html=True)
        st.markdown(f"<div class='insight-box red'>Kerosene/petrol ratio above 75% signals high energy burden for low-income households. Policy intervention threshold.</div>", unsafe_allow_html=True)
        fig_kw = go.Figure()
        fig_kw.add_hrect(y0=75, y1=115, fillcolor=hex_rgba(RED, 0.07), line_width=0,
                         annotation_text="High-risk zone (>75%)", annotation_position="top left",
                         annotation_font=dict(color=RED, size=11))
        fig_kw.add_trace(go.Scatter(
            x=pivot["price_date"], y=pivot["Kerosene/Petrol %"],
            mode="lines+markers", name="Kerosene/Petrol %",
            line=dict(color=AMBER, width=2.5), marker=dict(size=4),
            fill="tozeroy", fillcolor=hex_rgba(AMBER, 0.08),
            hovertemplate="<b>%{x|%b %Y}</b><br>%{y:.1f}%<extra></extra>",
        ))
        fig_kw.add_hline(y=75, line_dash="dot", line_color=RED, line_width=1.2)
        _fig(fig_kw, height=380, title="Kerosene Welfare Index (% of Petrol Price)")
        fig_kw.update_yaxes(title_text="% of Petrol Price", range=[0, 115])
        st.plotly_chart(fig_kw, use_container_width=True)

    st.markdown('<div class="section-title">Cross-fuel correlation — latest snapshot</div>', unsafe_allow_html=True)
    corr = cross_fuel_corr(df_latest)
    cf1, cf2 = st.columns(2)
    if "Petrol" in corr.columns and "Diesel" in corr.columns:
        with cf1:
            fig_s1 = px.scatter(corr, x="Petrol", y="Diesel", hover_data=["town","county"], color="county",
                                labels={"Petrol":"Petrol (KES)","Diesel":"Diesel (KES)"})
            _fig(fig_s1, height=420, title="Petrol vs Diesel — Latest")
            st.plotly_chart(fig_s1, use_container_width=True)
    if "Petrol" in corr.columns and "Kerosene" in corr.columns:
        with cf2:
            fig_s2 = px.scatter(corr, x="Petrol", y="Kerosene", hover_data=["town","county"], color="county",
                                labels={"Petrol":"Petrol (KES)","Kerosene":"Kerosene (KES)"})
            _fig(fig_s2, height=420, title="Petrol vs Kerosene — Latest")
            st.plotly_chart(fig_s2, use_container_width=True)

    st.markdown('<div class="section-title">All products — absolute price trend</div>', unsafe_allow_html=True)
    fig_all = go.Figure()
    for fuel in sel_fuels:
        sub = nat_trnd[nat_trnd["fuel"]==fuel].sort_values("price_date")
        fig_all.add_trace(go.Scatter(
            x=sub["price_date"], y=sub["avg"].round(2), name=fuel, mode="lines",
            line=dict(color=FUEL_COLORS[fuel], width=2),
            hovertemplate=f"<b>{fuel}</b> %{{x|%b %Y}}: KES %{{y:.2f}}<extra></extra>",
        ))
    _fig(fig_all, height=380, title="All Products — National Average Price")
    fig_all.update_yaxes(title_text="KES / litre")
    st.plotly_chart(fig_all, use_container_width=True)


# ───────────────────────────────────────────────────────────────
# TAB 6 — RAW DATA
# ───────────────────────────────────────────────────────────────
with tab6:
    st.markdown('<div class="section-title">Summary statistics</div>', unsafe_allow_html=True)
    sc = st.columns(len(sel_fuels))
    for col_s, fuel in zip(sc, sel_fuels):
        sub = flt[flt["fuel"]==fuel]["price"]
        with col_s:
            st.markdown(f"""
            <div class='stat-card'>
                <b>{fuel}</b><br>
                Mean: KES {sub.mean():.2f}<br>
                Median: KES {sub.median():.2f}<br>
                Min: KES {sub.min():.2f}<br>
                Max: KES {sub.max():.2f}<br>
                Std Dev: KES {sub.std():.2f}
            </div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-title">Filtered dataset</div>', unsafe_allow_html=True)
    export = (
        flt[["price_date","county","town","fuel","price"]]
        .sort_values(["price_date","county","town","fuel"])
        .rename(columns={"price_date":"Date","county":"County","town":"Town",
                         "fuel":"Fuel","price":"Price (KES)"})
        .copy()
    )
    export["Price (KES)"] = export["Price (KES)"].round(2)
    st.dataframe(export, use_container_width=True, hide_index=True)
    st.download_button("⬇ Download CSV", data=export.to_csv(index=False).encode("utf-8"),
                       file_name="kenya_fuel_prices.csv", mime="text/csv",
                       use_container_width=True)


# ═══════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
st.markdown(
    f"<p style='color:{TEXT_MUT};font-size:11px;text-align:center'>"
    "Kenya Fuel Price Intelligence · Data: EPRA gazette extracts & market estimates · "
    "Prices in Kenya Shillings (KES) · Built with Streamlit</p>",
    unsafe_allow_html=True,
)

