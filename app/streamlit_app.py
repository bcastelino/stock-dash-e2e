from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

repo_root = Path(__file__).resolve().parents[1] if "__file__" in globals() else Path.cwd()
sys.path.insert(0, str(repo_root / "src"))

from stock_dash_etl.config import load_config
from stock_dash_etl.metrics import read_gold_for_ui

st.set_page_config(page_title="Stock Dash ETL Monitor", layout="wide")

config = load_config()
gold_path = config.ui_fallback_gold_file

def _load_frame(path: Path) -> pd.DataFrame:
    frame = read_gold_for_ui(path)
    if frame.empty:
        columns = [
            "symbol",
            "latest_event_ts",
            "latest_close_price",
            "price_change",
            "price_change_pct",
            "avg_close_price",
            "max_volume",
            "minutes_since_ingestion",
        ]
        return pd.DataFrame(columns=columns)
    return frame


def _metric_value(frame: pd.DataFrame, column: str) -> str:
    if frame.empty or column not in frame.columns:
        return "n/a"
    value = frame[column].iloc[0]
    if pd.isna(value):
        return "n/a"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return str(value)


frame = _load_frame(gold_path)

st.title("Databricks Free ETL Pipeline Monitor")
st.caption("Alpha Vantage -> Bronze -> Silver -> Gold -> Streamlit")

with st.sidebar:
    st.subheader("Configuration")
    st.write(f"Symbols: {', '.join(config.symbols)}")
    st.write(f"Interval: {config.interval}")
    st.write(f"Gold export: {gold_path}")
    st.write("Preferred host: Databricks Apps")
    st.write("Fallback host: Streamlit Cloud")

if frame.empty:
    st.warning("No Gold export found yet. Run the Bronze, Silver, and Gold pipeline scripts first.")
    st.stop()

symbol_options = frame["symbol"].sort_values().unique().tolist()
selected_symbol = st.selectbox("Select symbol", symbol_options)
selected = frame.loc[frame["symbol"] == selected_symbol].copy()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Latest close", _metric_value(selected, "latest_close_price"))
col2.metric("Price change", _metric_value(selected, "price_change"))
col3.metric("Change %", _metric_value(selected, "price_change_pct"))
col4.metric("Minutes since ingestion", _metric_value(selected, "minutes_since_ingestion"))

left, right = st.columns([2, 1])

with left:
    chart_frame = frame[["symbol", "latest_close_price", "avg_close_price", "max_volume"]].copy()
    chart = px.bar(
        chart_frame,
        x="symbol",
        y="latest_close_price",
        color="symbol",
        title="Latest close price by symbol",
    )
    st.plotly_chart(chart, use_container_width=True)

with right:
    st.subheader("Latest Gold rows")
    st.dataframe(
        frame[[
            "symbol",
            "latest_event_ts",
            "latest_close_price",
            "price_change_pct",
            "max_volume",
            "minutes_since_ingestion",
        ]],
        use_container_width=True,
        hide_index=True,
    )

freshness_chart = px.scatter(
    frame,
    x="symbol",
    y="minutes_since_ingestion",
    size="max_volume",
    color="price_change_pct",
    title="Freshness and market activity",
)
st.plotly_chart(freshness_chart, use_container_width=True)
