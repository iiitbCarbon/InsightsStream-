"""Streamlit dashboard over the local MinIO gold Delta table."""

from __future__ import annotations

import sys
from pathlib import Path

import plotly.express as px
import streamlit as st
from py4j.protocol import Py4JJavaError
from pyspark.errors import AnalysisException

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from insightsstream.delta_pipeline import (
    DeltaSettings,
    create_delta_spark_session,
    delta_history,
)

st.set_page_config(page_title="InsightsStream Delta", page_icon="📊", layout="wide")
st.title("InsightsStream Local Delta Lakehouse")
st.caption("Batch CSV and Kafka streaming data stored in MinIO Delta tables")


@st.cache_resource
def spark_session():
    return create_delta_spark_session("InsightsStream-Delta-Dashboard")


settings = DeltaSettings.from_env()
spark = spark_session()

try:
    gold = spark.read.format("delta").load(
        settings.uri("gold/daily_metrics")
    ).toPandas()
except (AnalysisException, Py4JJavaError) as exc:
    st.error(f"Gold Delta table is unavailable: {exc}")
    st.info("Run scripts\\start-local-demo.ps1 first.")
    st.stop()

metrics = sorted(gold["metric"].unique())
selected = st.multiselect("Metrics", metrics, default=metrics)
filtered = gold[gold["metric"].isin(selected)]

col1, col2, col3 = st.columns(3)
col1.metric("Events", f"{int(filtered['event_count'].sum()):,}")
col2.metric("Total value", f"{filtered['total_value'].sum():,.2f}")
col3.metric("Delta versions", len(delta_history(spark, "gold/daily_metrics", settings).collect()))

st.plotly_chart(
    px.bar(
        filtered,
        x="event_date",
        y="total_value",
        color="metric",
        barmode="group",
        title="Daily value from the gold Delta table",
    ),
    use_container_width=True,
)
st.dataframe(filtered, use_container_width=True, hide_index=True)

with st.expander("Gold Delta transaction history"):
    st.dataframe(
        delta_history(spark, "gold/daily_metrics", settings)
        .select("version", "timestamp", "operation", "operationMetrics")
        .toPandas(),
        use_container_width=True,
        hide_index=True,
    )
