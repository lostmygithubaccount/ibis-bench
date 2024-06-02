import ibis
import gcsfs
import streamlit as st
import plotly.express as px

from ibis_bench.utils.monitor import get_timings_dir

st.set_page_config(layout="wide")
st.title("WIP Ibis benchmarking")
details = """
work in progress...data takes a bit to load from GCS...

the purpose of this dashboard is to compare TPC-H benchmarks across the big three single-node, Apache Arrow-based, modern OLAP engines: DuckDB, DataFusion, and Polars

current state is I have data from running on my M1 32GB RAM MacBook Pro, shown below

TODOs include:

- [ ] add the remaining TPC-H queries
- [ ] add DuckDB and DataFusion native
- [ ] run on cloud VM(s) for reproducibility
- [ ] open various issues on respective repos for bugs/improvements identified
- [ ] sanity-check and finalize CPU/memory usage data + add to visualizations
- [ ] generally improve the visualizations; make the app more interactive
- [ ] write up a (very easily reproduceable) blog post

TPC-H data is stored locally in Parquet format. the resulting benchmark data is stored in GCS in JSON format and read in via Ibis here
"""
details = details.strip()
st.markdown(details)

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
        title=f"scale factor: {sf} (~{sf} GB of data in memory; ~{sf*2/5}GB on disk in Parquet)",
    )
    st.plotly_chart(c)
