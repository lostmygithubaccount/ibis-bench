import ibis
import gcsfs
import streamlit as st
import plotly.express as px

from ibis_bench.utils.monitor import get_timings_dir

st.set_page_config(layout="wide")
st.title("WIP Ibis benchmarking")
st.write("work in progress...takes a bit to load...")

ibis.options.interactive = True
ibis.options.repr.interactive.max_rows = 20
ibis.options.repr.interactive.max_columns = None

# dark mode for px
px.defaults.template = "plotly_dark"

PROJECT = "voltrondata-demo"
BUCKET = "ibis-benchy"

fs = gcsfs.GCSFileSystem(project=PROJECT)

con = ibis.connect("duckdb://")
con.register_filesystem(fs)

t = (
    con.read_json(f"gs://{BUCKET}/{get_timings_dir()}/*.json", ignore_errors=True)
    .mutate(
        timestamp=ibis._["timestamp"].cast("timestamp"),
    )
    .cache()
)

agg = (
    t.filter(t["sf"] >= 1)
    # .filter((t["system"].contains("duckdb")) | (t["system"].contains("datafusion")))
    # .filter(t["query_number"] == 1)
    .group_by("system", "sf", "n_partitions", "query_number")
    .agg(
        mean_execution_seconds=t["execution_seconds"].mean(),
        max_peak_cpu=t["peak_cpu"].max(),
        max_peak_memory=t["peak_memory"].max(),
    )
    .order_by(
        ibis.desc("sf"),
        ibis.asc("n_partitions"),
        ibis.asc("query_number"),
        ibis.desc("system"),
        ibis.asc("mean_execution_seconds"),
    )
)

sfs = agg.select("sf").distinct().to_pandas()["sf"].tolist()

for sf in sorted(sfs):
    c = px.bar(
        agg.filter(agg["sf"] == sf),
        x="query_number",
        y="mean_execution_seconds",
        color="system",
        barmode="group",
        title=f"Scale Factor {sf}",
    )
    st.plotly_chart(c)
