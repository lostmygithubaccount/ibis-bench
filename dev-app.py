import ibis
import gcsfs
import streamlit as st
import plotly.express as px

from ibis_bench.utils.monitor import get_timings_dir, get_cache_dir, get_raw_json_dir  # noqa

st.set_page_config(layout="wide")
st.title("WIP Ibis benchmarking")
details = """
work in progress...data takes a bit to load from GCS...

**DATA IS NOT FINALIZED**

the purpose of this dashboard is to compare TPC-H benchmarks across the big three single-node, Apache Arrow-based, modern OLAP engines: DuckDB, DataFusion, and Polars

TODOs include:

- [x] add the remaining TPC-H queries
- [x] add DuckDB and DataFusion native (added via Ibis SQL)
- [x] run on cloud VM(s) for reproducibility
- [x] open various issues on respective repos for bugs/improvements identified
- [ ] sanity-check and finalize CPU/memory usage data + add to visualizations
- [ ] generally improve the visualizations; make the app more interactive
- [ ] write up a (very easily reproduceable) blog post

additional TODOs:

- [x] add CSV
- [ ] double-check Polars queries are up to date
- [ ] triple-check query correctness across
- [ ] final benchmarking on laptop(s) + VM(s)
"""
details = details.strip()
st.markdown(details)

ibis.options.interactive = True
ibis.options.repr.interactive.max_rows = 20
ibis.options.repr.interactive.max_columns = None

# dark mode for px
px.defaults.template = "plotly_dark"

# ibis connection
con = ibis.connect("duckdb://app.ddb")

# cloud logs
cloud = True


if cloud:
    PROJECT = "voltrondata-demo"
    BUCKET = "ibis-bench"
    # BUCKET = "ibis-benchy"

    fs = gcsfs.GCSFileSystem(project=PROJECT)

    con.register_filesystem(fs)

    glob = f"gs://{BUCKET}/{get_cache_dir()}/*.parquet"
else:
    glob = f"{get_cache_dir()}/*.parquet"


# read data
if "cache" not in con.list_tables():
    t = (
        con.read_parquet(glob)
        .mutate(
            timestamp=ibis._["timestamp"].cast("timestamp"),
        )
        .cache()
    )
    con.create_table("cache", t)
else:
    t = con.table("cache")


# streamlit viz beyond this point
cols = st.columns(6)
with cols[0]:
    st.metric(
        label="total queries run",
        value=t.count().to_pandas(),
    )
with cols[1]:
    st.metric(
        label="total queries run (parquet)",
        value=t.filter(t["file_type"] == "parquet").count().to_pandas(),
    )
with cols[2]:
    st.metric(
        label="total queries run (csv)",
        value=t.filter(t["file_type"] == "csv").count().to_pandas(),
    )
with cols[3]:
    st.metric(
        label="total runtime minutes",
        value=round(t["execution_seconds"].sum().to_pandas() / 60, 2),
    )
with cols[4]:
    st.metric(
        label="total systems",
        value=t.select("system").distinct().count().to_pandas(),
    )
with cols[5]:
    st.metric(
        label="total queries",
        value=t.select("query_number").distinct().count().to_pandas(),
    )

with st.form(key="app"):
    # system options
    system_options = sorted(
        t.select("system").distinct().to_pandas()["system"].tolist()
    )
    system = st.multiselect(
        "Select systems",
        system_options,
        default=system_options,
    )

    # filetype options
    filetype_options = sorted(
        t.select("file_type").distinct().to_pandas()["file_type"].tolist()
    )
    file_type = st.multiselect(
        "Select a file type",
        filetype_options,
        default=filetype_options,
    )

    # instance type options
    instance_types = sorted(
        t.select("instance_type").distinct().to_pandas()["instance_type"].tolist()
    )
    instance_type = st.radio(
        "Select an instance type",
        instance_types,
        # index=instance_types.index("work laptop"),
    )

    # query options
    query_numbers = sorted(
        t.select("query_number").distinct().to_pandas()["query_number"].tolist()
    )
    start_query, end_query = st.select_slider(
        "Select a range of queries",
        options=query_numbers,
        value=(min(query_numbers), max(query_numbers)),
    )

    # submit button
    update_button = st.form_submit_button(label="update")

# parquet

agg = (
    t.filter(t["sf"] >= 1)  # TODO: change back to 1
    .filter(t["system"].isin(system))
    .filter(t["file_type"].isin(file_type))
    # .filter(t["file_type"] == file_type)
    .filter(t["instance_type"] == instance_type)
    .filter(t["query_number"] >= start_query)
    .filter(t["query_number"] <= end_query)
    .group_by("system", "sf", "query_number", "file_type")
    .agg(
        mean_execution_seconds=t["execution_seconds"].mean(),
        max_peak_cpu=t["peak_cpu"].max(),
        max_peak_memory=t["peak_memory"].max(),
    )
    .order_by(
        ibis.desc("sf"),
        ibis.asc("query_number"),
        ibis.desc("system"),
        ibis.asc("mean_execution_seconds"),
        ibis.asc("file_type"),
    )
)

sfs = agg.select("sf").distinct().to_pandas()["sf"].tolist()
category_orders = {
    "query_number": sorted(
        agg.select("query_number").distinct().to_pandas()["query_number"].tolist()
    ),
    "system": sorted(agg.select("system").distinct().to_pandas()["system"].tolist()),
}

for sf in sorted(sfs):
    c = px.bar(
        agg.filter(agg["sf"] == sf),
        x="query_number",
        y="mean_execution_seconds",
        color="system",
        category_orders=category_orders,
        barmode="group",
        pattern_shape="file_type",
        title=f"scale factor: {sf} (~{sf}GB in memory; ~{round(sf * 2/5, 2)}GB as Parquet; ~{round(sf * 11/10, 2)}GB as CSV)",
    )
    st.plotly_chart(c)
