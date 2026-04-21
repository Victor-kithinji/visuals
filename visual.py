"""
Kenya Fuel Price Executive Dashboard
=====================================
Streamlit app pulling from PostgreSQL.
Requires: streamlit, psycopg2-binary, pandas, plotly, sqlalchemy, python-dotenv
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Kenya Fuel Price Dashboard",
    page_icon="⛽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# THEME TOKENS  (light / dark agnostic via CSS variables)
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap');

/* ── global ── */
html, body, [data-testid="stAppViewContainer"], [data-testid="stMain"] {
    background: #0d1117 !important;
    font-family: 'Inter', sans-serif;
    color: #e2e8f0;
}
[data-testid="stSidebar"] {
    background: #161b27 !important;
    border-right: 1px solid #1f2937;
}
[data-testid="stSidebar"] * { color: #94a3b8 !important; }
[data-testid="stSidebar"] .stMarkdown h2 { color: #f1f5f9 !important; font-size: 14px !important; letter-spacing: .04em; }
[data-testid="stSidebar"] hr { border-color: #1f2937 !important; }
[data-testid="stSidebar"] label { color: #64748b !important; font-size: 11px !important; }
h1,h2,h3,h4 { font-weight: 500 !important; color: #f1f5f9 !important; }
p, li, span { color: #94a3b8; }
hr { border-color: #1f2937 !important; }

/* ── KPI cards ── */
.kpi-card {
    background: #161b27;
    border: 1px solid #1f2937;
    border-radius: 14px;
    padding: 20px 22px;
    margin-bottom: 4px;
}
.kpi-label {
    font-size: 10px;
    letter-spacing: .09em;
    color: #475569;
    text-transform: uppercase;
    margin-bottom: 8px;
    font-weight: 600;
}
.kpi-value {
    font-size: 28px;
    font-weight: 600;
    color: #f1f5f9;
    line-height: 1.1;
    letter-spacing: -.02em;
}
.kpi-delta-up  { font-size: 12px; color: #f87171; margin-top: 6px; font-weight: 500; }
.kpi-delta-down{ font-size: 12px; color: #4ade80; margin-top: 6px; font-weight: 500; }
.kpi-delta-neu { font-size: 12px; color: #475569; margin-top: 6px; }

/* ── section header ── */
.section-title {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: .1em;
    text-transform: uppercase;
    color: #475569;
    margin: 32px 0 12px;
    padding-bottom: 8px;
    border-bottom: 1px solid #1f2937;
}

/* ── insight card ── */
.insight-card {
    background: #161b27;
    border: 1px solid #1f2937;
    border-radius: 12px;
    padding: 18px 20px;
    margin-bottom: 4px;
    height: 100%;
}
.insight-label {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: .09em;
    text-transform: uppercase;
    color: #475569;
    margin-bottom: 6px;
}
.insight-value  { font-size: 17px; font-weight: 600; color: #f1f5f9; line-height: 1.2; }
.insight-detail { font-size: 12px; color: #64748b; margin-top: 6px; line-height: 1.6; }

/* hide default streamlit footer */
footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────
@st.cache_resource(show_spinner="Connecting to database…")
def get_engine():
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/defaultdb")
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif db_url.startswith("postgresql://") and "+psycopg2" not in db_url:
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return create_engine(db_url, pool_pre_ping=True)


# ─────────────────────────────────────────────
# DATA LOADERS  (cached per session)
# ─────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="Loading price data…")
def load_data(_engine, start_year: int, end_year: int,
              counties: list[str]) -> pd.DataFrame:
    county_filter = (
        "AND county = ANY(:counties)" if counties else ""
    )
    sql = text(f"""
        SELECT
            canonical_month,
            period_start,
            town,
            county,
            super_petrol,
            diesel,
            kerosene,
            source,
            data_quality,
            extraction_method
        FROM fuel_price_rows
        WHERE EXTRACT(YEAR FROM canonical_month::date) BETWEEN :sy AND :ey
          {county_filter}
        ORDER BY canonical_month, county, town
    """)
    params = {"sy": start_year, "ey": end_year}
    if counties:
        params["counties"] = counties
    with _engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)
    df["canonical_month"] = pd.to_datetime(df["canonical_month"])
    df["year"] = df["canonical_month"].dt.year
    df["month"] = df["canonical_month"].dt.month
    return df


@st.cache_data(ttl=300)
def load_counties(_engine) -> list[str]:
    sql = text("SELECT DISTINCT county FROM fuel_price_rows ORDER BY county")
    with _engine.connect() as conn:
        result = conn.execute(sql)
        return [row[0] for row in result]


@st.cache_data(ttl=300)
def load_year_range(_engine) -> tuple[int, int]:
    sql = text("""
        SELECT
            EXTRACT(YEAR FROM MIN(canonical_month::date))::int,
            EXTRACT(YEAR FROM MAX(canonical_month::date))::int
        FROM fuel_price_rows
    """)
    with _engine.connect() as conn:
        result = conn.execute(sql).fetchone()
        return int(result[0]), int(result[1])


# ─────────────────────────────────────────────
# HELPER: KPI card HTML
# ─────────────────────────────────────────────
def kpi_card(label: str, value: str, delta: str = "",
             delta_dir: str = "neu") -> str:
    delta_cls = f"kpi-delta-{delta_dir}"
    delta_html = f'<div class="{delta_cls}">{delta}</div>' if delta else ""
    return f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {delta_html}
    </div>"""


# ─────────────────────────────────────────────
# CHART HELPERS
# ─────────────────────────────────────────────
COLORS = {
    "super_petrol": "#378ADD",
    "diesel":       "#1D9E75",
    "kerosene":     "#D85A30",
}

GRID = dict(gridcolor="#1f2937", linecolor="#1f2937", tickfont=dict(size=11, color="#475569"))

PLOT_LAYOUT = dict(
    paper_bgcolor="#161b27",
    plot_bgcolor="#161b27",
    font=dict(family="Inter, sans-serif", color="#94a3b8", size=12),
    margin=dict(l=0, r=0, t=36, b=0),
    legend=dict(
        orientation="h", yanchor="bottom", y=1.02,
        xanchor="left", x=0,
        bgcolor="rgba(0,0,0,0)",
        font=dict(size=11, color="#94a3b8"),
    ),
)


def apply_axes(fig):
    fig.update_xaxes(**GRID)
    fig.update_yaxes(**GRID)
    return fig


def trend_chart(df: pd.DataFrame, benchmark_county: str = "Nairobi"):
    """20-year price trajectory for a given county."""
    filt = df[df["county"] == benchmark_county].copy()
    monthly = (
        filt.groupby("canonical_month")[["super_petrol", "diesel", "kerosene"]]
        .mean()
        .reset_index()
    )

    fig = go.Figure()
    for col, name, dash in [
        ("super_petrol", "Super petrol", "solid"),
        ("diesel",       "Diesel",       "dash"),
        ("kerosene",     "Kerosene",     "dot"),
    ]:
        fig.add_trace(go.Scatter(
            x=monthly["canonical_month"], y=monthly[col].round(1),
            name=name, mode="lines",
            line=dict(color=COLORS[col], dash=dash, width=2),
            fill="tozeroy" if col == "super_petrol" else "none",
            fillcolor="rgba(55,138,221,0.07)",
            hovertemplate="%{y:.1f} KES<extra>" + name + "</extra>",
        ))

    fig.update_layout(**PLOT_LAYOUT, yaxis_title="KES / litre", hovermode="x unified", height=320)
    apply_axes(fig)
    return fig


def regional_bar_chart(df: pd.DataFrame, fuel: str = "super_petrol",
                       period_label: str = "Selected period"):
    """Average price by county — horizontal bar."""
    by_county = (
        df.groupby("county")[fuel].mean().reset_index()
        .rename(columns={fuel: "price"})
        .sort_values("price", ascending=True)
    )
    by_county["price"] = by_county["price"].round(1)

    fig = px.bar(
        by_county, x="price", y="county", orientation="h",
        color="price",
        color_continuous_scale=["#9FE1CB", "#EF9F27", "#D85A30"],
        labels={"price": "KES / litre", "county": ""},
        text="price",
    )
    fig.update_traces(textposition="outside", texttemplate="%{text:.1f}")
    fig.update_layout(
        **PLOT_LAYOUT,
        coloraxis_showscale=False,
        height=max(320, len(by_county) * 24),
        xaxis_title="KES / litre",
    )
    apply_axes(fig)
    return fig


def ratio_chart(df: pd.DataFrame, benchmark_county: str = "Nairobi"):
    """Diesel/petrol and kerosene/petrol ratio over time."""
    filt = df[df["county"] == benchmark_county].copy()
    monthly = (
        filt.groupby("canonical_month")[["super_petrol", "diesel", "kerosene"]]
        .mean()
        .reset_index()
    )
    monthly["diesel_ratio"]   = (monthly["diesel"]   / monthly["super_petrol"] * 100).round(1)
    monthly["kerosene_ratio"] = (monthly["kerosene"] / monthly["super_petrol"] * 100).round(1)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=monthly["canonical_month"], y=monthly["diesel_ratio"],
        name="Diesel / Petrol %",
        line=dict(color=COLORS["diesel"], dash="dash", width=2),
        hovertemplate="%{y:.1f}%<extra>Diesel/Petrol</extra>",
    ))
    fig.add_trace(go.Scatter(
        x=monthly["canonical_month"], y=monthly["kerosene_ratio"],
        name="Kerosene / Petrol %",
        line=dict(color=COLORS["kerosene"], width=2),
        fill="tozeroy", fillcolor="rgba(216,90,48,0.07)",
        hovertemplate="%{y:.1f}%<extra>Kerosene/Petrol</extra>",
    ))
    fig.update_layout(**PLOT_LAYOUT, hovermode="x unified", height=280)
    apply_axes(fig)
    fig.update_yaxes(title_text="Ratio (%)", range=[30, 100])
    return fig


def yoy_change_chart(df: pd.DataFrame, benchmark_county: str = "Nairobi",
                     fuel_cols: list = None):
    """Year-on-year % change for one or all fuels."""
    if fuel_cols is None:
        fuel_cols = ["super_petrol"]
    filt = df[df["county"] == benchmark_county].copy()
    fig = go.Figure()
    for col in fuel_cols:
        annual = filt.groupby("year")[col].mean().reset_index()
        annual["yoy"] = annual[col].pct_change() * 100
        annual = annual.dropna()
        if len(fuel_cols) == 1:
            annual["color"] = annual["yoy"].apply(lambda v: "#D85A30" if v > 0 else "#1D9E75")
            fig.add_trace(go.Bar(
                x=annual["year"], y=annual["yoy"].round(1),
                marker_color=annual["color"],
                text=annual["yoy"].round(1),
                texttemplate="%{text:.1f}%",
                textposition="outside",
                hovertemplate="Year %{x}<br>YoY: %{y:.1f}%<extra></extra>",
                showlegend=False,
            ))
        else:
            fig.add_trace(go.Scatter(
                x=annual["year"], y=annual["yoy"].round(1),
                mode="lines+markers",
                name=col.replace("_", " ").title().replace("Super Petrol", "Petrol"),
                line=dict(color=COLORS[col], width=2),
                marker=dict(size=5),
                hovertemplate=f"Year %{{x}}<br>YoY: %{{y:.1f}}%<extra>{col}</extra>",
            ))
    fig.add_hline(y=0, line_width=0.8, line_color="#334155")
    fig.update_layout(**PLOT_LAYOUT, yaxis_title="YoY change (%)", height=260)
    apply_axes(fig)
    return fig


def spread_over_time(df: pd.DataFrame):
    """Max − Min price spread across all counties per month."""
    monthly_spread = (
        df.groupby("canonical_month")["super_petrol"]
        .agg(["min", "max"])
        .reset_index()
    )
    monthly_spread["spread"] = (monthly_spread["max"] - monthly_spread["min"]).round(1)

    fig = go.Figure(go.Scatter(
        x=monthly_spread["canonical_month"],
        y=monthly_spread["spread"],
        fill="tozeroy",
        fillcolor="rgba(127,119,221,0.12)",
        line=dict(color="#534AB7", width=1.5),
        hovertemplate="Spread: %{y:.1f} KES<extra></extra>",
    ))
    fig.update_layout(**PLOT_LAYOUT, yaxis_title="KES spread", height=220)
    apply_axes(fig)
    return fig


def data_quality_donut(df: pd.DataFrame):
    counts = df["data_quality"].value_counts().reset_index()
    counts.columns = ["quality", "count"]
    fig = px.pie(
        counts, names="quality", values="count", hole=0.6,
        color_discrete_sequence=["#378ADD", "#1D9E75", "#EF9F27", "#D85A30"],
    )
    fig.update_traces(textinfo="label+percent", textfont_size=11)
    fig.update_layout(**PLOT_LAYOUT, showlegend=False, height=220)
    fig.update_layout(margin=dict(l=0, r=0, t=10, b=0))
    return fig


# ─────────────────────────────────────────────
# MAIN APP
# ─────────────────────────────────────────────
def main():
    # ── connection ──────────────────────────────
    try:
        engine = get_engine()
        min_year, max_year = load_year_range(engine)
        all_counties = load_counties(engine)
    except Exception as e:
        st.error(f"**Database connection failed.** Check your DATABASE_URL environment variable.\n\n`{e}`")
        st.info("Set DATABASE_URL in a `.env` file or as an environment variable:\n```\nDATABASE_URL=postgresql://user:password@host:5432/dbname\n```")
        st.stop()

    # ── sidebar controls ─────────────────────────
    with st.sidebar:
        st.markdown("## ⛽ Fuel Dashboard")
        st.markdown("---")

        st.markdown("#### Time range")
        year_range = st.slider(
            "Year range", min_year, max_year,
            (min_year, max_year), label_visibility="collapsed"
        )

        st.markdown("#### Counties")
        county_select = st.multiselect(
            "Filter counties (blank = all)",
            options=all_counties,
            default=[],
            label_visibility="collapsed",
            placeholder="All counties",
        )

        st.markdown("#### Benchmark county")
        benchmark = st.selectbox(
            "For trend charts", options=all_counties,
            index=all_counties.index("Nairobi") if "Nairobi" in all_counties else 0,
            label_visibility="collapsed",
        )

        st.markdown("#### Fuel type")
        fuel_map = {
            "Super petrol": "super_petrol",
            "Diesel":       "diesel",
            "Kerosene":     "kerosene",
            "All":          None,
        }
        fuel_label = st.radio(
            "For regional chart", list(fuel_map.keys()),
            label_visibility="collapsed",
        )
        fuel_col = fuel_map[fuel_label]
        ALL_COLS = ["super_petrol", "diesel", "kerosene"]
        active_cols = ALL_COLS if fuel_col is None else [fuel_col]

        st.markdown("---")
        if st.button("🔄 Refresh data"):
            st.cache_data.clear()
            st.rerun()

    # ── load data ────────────────────────────────
    with st.spinner("Querying database…"):
        df = load_data(engine, year_range[0], year_range[1], county_select)

    if df.empty:
        st.warning("No data returned for the selected filters.")
        st.stop()

    # ── derived metrics ───────────────────────────
    bench_df    = df[df["county"] == benchmark]
    latest_mo   = df["canonical_month"].max()
    earliest_mo = df["canonical_month"].min()
    latest_bench   = bench_df[bench_df["canonical_month"] == latest_mo]
    earliest_bench = bench_df[bench_df["canonical_month"] == earliest_mo]
    latest_all  = df[df["canonical_month"] == latest_mo]

    def fuel_metric(col):
        latest_v   = latest_bench[col].mean()
        earliest_v = earliest_bench[col].mean()
        pct        = ((latest_v - earliest_v) / earliest_v * 100) if earliest_v else 0
        county_s   = df.groupby("county")[col].mean()
        spread_v   = latest_all[col].max() - latest_all[col].min()
        return latest_v, earliest_v, pct, county_s.idxmax(), county_s.max(), spread_v

    # Kerosene ratio is always vs petrol
    latest_petrol = latest_bench["super_petrol"].mean()
    kero_ratio = (latest_bench["kerosene"].mean() / latest_petrol * 100) if latest_petrol else 0

    # ── header ───────────────────────────────────
    st.markdown(
        f"## Kenya Fuel Price Intelligence &nbsp;&nbsp;"
        f"<span style='font-size:14px;color:#475569;font-weight:400;'>"
        f"EPRA pump price data · {year_range[0]}–{year_range[1]} · "
        f"{df['county'].nunique()} counties · {len(df):,} records</span>",
        unsafe_allow_html=True,
    )
    st.markdown("---")

    # ── KPI row ───────────────────────────────────
    k1, k2, k3, k4, k5 = st.columns(5)

    if fuel_col is None:
        # "All" mode — one card per fuel + spread + records
        fuel_display = [("Super Petrol", "super_petrol"), ("Diesel", "diesel"), ("Kerosene", "kerosene")]
        for slot, (fname, fcol) in zip([k1, k2, k3], fuel_display):
            lv, _, pct, _, _, _ = fuel_metric(fcol)
            with slot:
                st.markdown(kpi_card(
                    f"{benchmark} {fname} (latest)",
                    f"KES {lv:.0f}",
                    f"{pct:+.0f}% vs {year_range[0]}",
                    "up" if pct > 0 else "down",
                ), unsafe_allow_html=True)
        avg_spread = sum(fuel_metric(c)[5] for c in ALL_COLS) / len(ALL_COLS)
        with k4:
            st.markdown(kpi_card(
                "Avg county spread (all fuels)",
                f"KES {avg_spread:.1f}",
                "Mean of max−min across fuels",
                "neu",
            ), unsafe_allow_html=True)
        with k5:
            st.markdown(kpi_card(
                "Records in selection",
                f"{len(df):,}",
                f"{df['canonical_month'].nunique()} months · {df['county'].nunique()} counties",
                "neu",
            ), unsafe_allow_html=True)
    else:
        lv, ev, pct, priciest_county, priciest_val, spread_val = fuel_metric(fuel_col)
        cheapest_county = df.groupby("county")[fuel_col].mean().idxmin()
        cheapest_val    = df.groupby("county")[fuel_col].mean().min()
        with k1:
            st.markdown(kpi_card(
                f"{benchmark} {fuel_label} (latest)",
                f"KES {lv:.0f}",
                f"{pct:+.0f}% vs {year_range[0]}",
                "up" if pct > 0 else "down",
            ), unsafe_allow_html=True)
        with k2:
            st.markdown(kpi_card(
                f"Most expensive county",
                priciest_county,
                f"KES {priciest_val:.0f} avg",
                "up",
            ), unsafe_allow_html=True)
        with k3:
            st.markdown(kpi_card(
                "County price spread (latest)",
                f"KES {spread_val:.1f}",
                "Max − min across all counties",
                "neu",
            ), unsafe_allow_html=True)
        with k4:
            if fuel_col == "super_petrol":
                st.markdown(kpi_card(
                    "Kerosene / petrol ratio",
                    f"{kero_ratio:.0f}%",
                    "Welfare affordability proxy",
                    "down" if kero_ratio < 60 else "neu",
                ), unsafe_allow_html=True)
            else:
                st.markdown(kpi_card(
                    "Most affordable county",
                    cheapest_county,
                    f"KES {cheapest_val:.0f} avg",
                    "down",
                ), unsafe_allow_html=True)
        with k5:
            st.markdown(kpi_card(
                "Records in selection",
                f"{len(df):,}",
                f"{df['canonical_month'].nunique()} months · {df['county'].nunique()} counties",
                "neu",
            ), unsafe_allow_html=True)

    # convenience alias for sections below that still need a scalar spread/kero_ratio
    if fuel_col is not None:
        _, _, _, _, _, spread_val = fuel_metric(fuel_col)
    else:
        spread_val = avg_spread

    # ── 20-year trend ─────────────────────────────
    st.markdown('<div class="section-title">20-year price trajectory</div>', unsafe_allow_html=True)
    st.plotly_chart(trend_chart(df, benchmark), use_container_width=True, config={"displayModeBar": False})

    # ── two-column row ────────────────────────────
    col_a, col_b = st.columns([1, 1])

    with col_a:
        yoy_title = "Year-on-year change · all fuels" if fuel_col is None else f"Year-on-year change · {fuel_label.lower()}"
        st.markdown(f'<div class="section-title">{yoy_title}</div>', unsafe_allow_html=True)
        st.plotly_chart(yoy_change_chart(df, benchmark, active_cols), use_container_width=True, config={"displayModeBar": False})

    with col_b:
        st.markdown('<div class="section-title">Fuel type ratio vs petrol</div>', unsafe_allow_html=True)
        st.plotly_chart(ratio_chart(df, benchmark), use_container_width=True, config={"displayModeBar": False})

    # ── regional spread ───────────────────────────
    col_c, col_d = st.columns([2, 1])

    with col_c:
        st.markdown(
            f'<div class="section-title">Regional price spread — {fuel_label}</div>',
            unsafe_allow_html=True,
        )
        if fuel_col is None:
            # All: grouped bar — one bar per fuel per county
            avg_by_county = (
                df.groupby("county")[ALL_COLS].mean().reset_index()
                .rename(columns={"super_petrol": "Super Petrol", "diesel": "Diesel", "kerosene": "Kerosene"})
            )
            melted = avg_by_county.melt(id_vars="county", var_name="Fuel", value_name="price")
            melted["price"] = melted["price"].round(1)
            melted = melted.sort_values("price")
            fig_all = px.bar(
                melted, x="price", y="county", color="Fuel", orientation="h",
                barmode="group",
                color_discrete_map={"Super Petrol": COLORS["super_petrol"],
                                    "Diesel": COLORS["diesel"], "Kerosene": COLORS["kerosene"]},
                labels={"price": "KES / litre", "county": ""},
            )
            fig_all.update_layout(**PLOT_LAYOUT, coloraxis_showscale=False,
                                  height=max(320, len(avg_by_county) * 32), xaxis_title="KES / litre")
            apply_axes(fig_all)
            st.plotly_chart(fig_all, use_container_width=True, config={"displayModeBar": False})
        else:
            st.plotly_chart(
                regional_bar_chart(df, fuel_col, f"{year_range[0]}–{year_range[1]}"),
                use_container_width=True,
                config={"displayModeBar": False},
            )

    with col_d:
        st.markdown('<div class="section-title">Inter-county spread over time</div>', unsafe_allow_html=True)
        st.plotly_chart(spread_over_time(df), use_container_width=True, config={"displayModeBar": False})

        st.markdown('<div class="section-title" style="margin-top:16px;">Data quality breakdown</div>', unsafe_allow_html=True)
        st.plotly_chart(data_quality_donut(df), use_container_width=True, config={"displayModeBar": False})

    # ── insights row ──────────────────────────────
    st.markdown('<div class="section-title">Key analytical findings</div>', unsafe_allow_html=True)

    i1, i2, i3, i4 = st.columns(4)
    insights = [
        ("Remoteness premium",
         f"KES {spread_val:.0f}/litre",
         "Persistent gap between urban centres and remote northern counties driven by last-mile logistics costs."),
        ("Estimated data period",
         "Pre-2010",
         "2006–2009 prices are market estimates, not EPRA gazette extracts. Use directionally, not for precision procurement."),
        ("Diesel convergence",
         "Post-2019",
         "Diesel/petrol ratio has trended toward parity, reflecting tax restructuring. Watch for margin squeeze on transport operators."),
        ("Welfare risk signal",
         f"Kerosene {kero_ratio:.0f}%",
         "When kerosene approaches petrol parity, low-income household energy burden spikes. Key input for social protection targeting."),
    ]
    for col, (label, value, detail) in zip([i1, i2, i3, i4], insights):
        with col:
            st.markdown(f"""
            <div class="insight-card">
                <div class="insight-label">{label}</div>
                <div class="insight-value">{value}</div>
                <div class="insight-detail">{detail}</div>
            </div>""", unsafe_allow_html=True)

    # ── raw data expander ─────────────────────────
    with st.expander("📋 Raw data sample (latest 500 rows)"):
        show_cols = ["canonical_month", "town", "county", "super_petrol",
                     "diesel", "kerosene", "source", "data_quality"]
        st.dataframe(
            df[show_cols].sort_values("canonical_month", ascending=False).head(500),
            use_container_width=True,
            hide_index=True,
        )

    # ── footer ────────────────────────────────────
    st.markdown("---")
    st.markdown(
        "<p style='font-size:11px;color:#888780;text-align:center;'>"
        "Source: EPRA pump price gazette extracts &amp; market estimates · "
        "energypedia.info/wiki/Fuel_Prices_Kenya</p>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()