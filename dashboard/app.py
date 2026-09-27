"""Streamlit dashboard for the latest InsightsStream gold dataset."""

from __future__ import annotations

import sys
from dataclasses import asdict
from pathlib import Path

import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from insightsstream.pipeline import load_latest_gold
from insightsstream.storage import create_store

st.set_page_config(page_title="InsightsStream", page_icon="📊", layout="wide")
st.title("InsightsStream Lakehouse Dashboard")
st.caption("Gold-layer analytics from the latest local or Cloudflare R2 pipeline run")

try:
    gold, run = load_latest_gold(create_store())
except (FileNotFoundError, ValueError) as exc:
    st.error(str(exc))
    st.info("Run `python -m insightsstream.cli data/source/sample_events.csv` first.")
    st.stop()

metrics = sorted(gold["metric"].unique())
selected_metrics = st.multiselect("Metrics", metrics, default=metrics)
filtered = gold[gold["metric"].isin(selected_metrics)].copy()

total_value = float(filtered["total_value"].sum())
total_events = int(filtered["event_count"].sum())
unique_entities = int(filtered["unique_entities"].sum())

col1, col2, col3, col4 = st.columns(4)
col1.metric("Batch", run.batch_id)
col2.metric("Valid events", f"{run.silver_rows:,}")
col3.metric("Rejected rows", f"{run.rejected_rows:,}")
col4.metric("Total value", f"{total_value:,.2f}")

st.plotly_chart(
    px.bar(
        filtered,
        x="event_date",
        y="total_value",
        color="metric",
        barmode="group",
        title="Daily value by metric",
        labels={"event_date": "Event date", "total_value": "Total value"},
    ),
    use_container_width=True,
)

left, right = st.columns(2)
with left:
    st.plotly_chart(
        px.pie(
            filtered,
            names="metric",
            values="event_count",
            title=f"Event mix ({total_events:,} events)",
        ),
        use_container_width=True,
    )
with right:
    st.plotly_chart(
        px.bar(
            filtered,
            x="metric",
            y="unique_entities",
            title=f"Unique entities by metric ({unique_entities:,} metric memberships)",
        ),
        use_container_width=True,
    )

st.subheader("Gold dataset")
st.dataframe(filtered, use_container_width=True, hide_index=True)

with st.expander("Pipeline run metadata"):
    st.json(asdict(run))
