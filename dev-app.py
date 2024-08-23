import ibis
import gcsfs
import tomllib
import streamlit as st
import plotly.express as px

from ibis_bench.utils.monitor import get_cache_dir

st.set_page_config(layout="wide")
st.title("Ibis benchmarking results")

with open("pyproject.toml", "rb") as f:
    pyproject = tomllib.load(f)

dependencies = pyproject["project"]["dependencies"]

datafusion_version = [d for d in dependencies if "datafusion" in d][0]
duckdb_version = [d for d in dependencies if "duckdb" in d][0]
polars_version = [d for d in dependencies if "polars" in d][0]
ibis_version = [d for d in dependencies if "ibis" in d][0]

details = f"""
[blog blog blog](https://ibis-project.org/posts/ibis-bench)

the purpose of this dashboard is to compare TPC-H benchmarks across the big three single-node, Apache Arrow-based, modern OLAP engines: DuckDB, DataFusion, and Polars

versions:

- DataFusion: `{datafusion_version}`
- DuckDB: `{duckdb_version}`
- Polars: `{polars_version}`
- Ibis: `{ibis_version}`
"""
details = details.strip()
st.markdown(details)

with open("results.py") as f:
    code = f.read()

with st.expander("Show source code for this page", expanded=False):
    st.code(code, line_numbers=True, language="python")

ibis.options.interactive = True
ibis.options.repr.interactive.max_rows = 20
ibis.options.repr.interactive.max_columns = None

# dark mode for px
px.defaults.template = "plotly_dark"


def get_t():
    # ibis connection
    # con = ibis.connect("duckdb://cache.ddb")
    con = ibis.connect("duckdb://")

    # cloud logs
    cloud = False

    if cloud:
        PROJECT = "voltrondata-demo"
        BUCKET = "ibis-bench"
        # BUCKET = "ibis-benchy"

        fs = gcsfs.GCSFileSystem(project=PROJECT)

        con.register_filesystem(fs)

        glob = f"gs://{BUCKET}/{get_cache_dir()}/*.parquet"
    else:
        glob = f"bench_logs_v2/raw_json/*.json"

    print(glob)

    # read data
    table_name = "bench_data"
    if table_name not in con.list_tables():
        t = (
            con.read_json(glob)
            .mutate(
                timestamp=ibis._["timestamp"].cast("timestamp"),
            )
            .drop("file_id")
            .distinct()  # TODO: hmmmmmm
            .filter(ibis._["file_type"] == "parquet")  # TODO: remove after CSV runs
            .mutate(instance_type=ibis.literal("work laptop"))
            .cache()
        )
        con.create_table(table_name, t)
    else:
        t = con.table(table_name)

    return t


def get_instance_details(t):
    cpu_type_cases = (
        ibis.case()
        .when(
            ibis._["instance_type"].startswith("n2d"),
            "AMD EPYC",
        )
        .when(
            ibis._["instance_type"].startswith("n2"),
            "Intel Cascade and Ice Lake",
        )
        .when(
            ibis._["instance_type"].startswith("c3"),
            "Intel Saphire Rapids",
        )
        .when(
            ibis._["instance_type"] == "work laptop",
            "Apple M1 Max",
        )
        .when(
            ibis._["instance_type"] == "personal laptop",
            "Apple M2 Max",
        )
        .else_("unknown")
        .end()
    )
    cpu_num_cases = (
        ibis.case()
        .when(
            ibis._["instance_type"].contains("-"),
            ibis._["instance_type"].split("-")[-1].cast("int"),
        )
        .when(ibis._["instance_type"].contains("laptop"), 12)
        .else_(0)
        .end()
    )
    memory_gb_cases = (
        ibis.case()
        .when(
            ibis._["instance_type"].contains("-"),
            ibis._["instance_type"].split("-")[-1].cast("int") * 4,
        )
        .when(ibis._["instance_type"] == "work laptop", 32)
        .when(ibis._["instance_type"] == "personal laptop", 96)
        .else_(0)
        .end()
    )

    instance_details = (
        t.group_by("instance_type")
        .agg()
        .mutate(
            cpu_type=cpu_type_cases, cpu_cores=cpu_num_cases, memory_gbs=memory_gb_cases
        )
    ).order_by("memory_gbs", "cpu_cores", "instance_type")

    return instance_details


t = get_t()
instance_details = get_instance_details(t)


# streamlit viz beyond this point
def totals_metrics(t):
    cols = st.columns(6)
    with cols[0]:
        st.metric(
            label="total queries run",
            value=f"{t.count().to_pandas():,}",
        )
    with cols[1]:
        st.metric(
            label="total runtime minutes",
            value=f"{round(t['execution_seconds'].sum().to_pandas() / 60, 2):,}",
            help=f"average: {round(t['execution_seconds'].mean().to_pandas(), 2)}s/query",
        )
    with cols[2]:
        st.metric(
            label="total systems",
            value=t.select("system").distinct().count().to_pandas(),
        )
    with cols[3]:
        st.metric(
            label="total instance types",
            value=t.select("instance_type").distinct().count().to_pandas(),
        )
    with cols[4]:
        st.metric(
            label="total scale factors",
            value=t.select("sf").distinct().count().to_pandas(),
        )
    with cols[5]:
        st.metric(
            label="total queries",
            value=t.select("query_number").distinct().count().to_pandas(),
        )


st.markdown("## totals (all data)")
totals_metrics(t)

# user options
st.markdown("## data filters")
with st.form(key="app"):
    # system options
    system_options = sorted(
        t.select("system").distinct().to_pandas()["system"].tolist()
    )
    systems = st.multiselect(
        "select system(s)",
        system_options,
        # default=system_options,
        default=["ibis-datafusion", "ibis-duckdb", "polars-lazy"],
    )

    instance_type_options = sorted(
        t.select("instance_type").distinct().to_pandas()["instance_type"].tolist(),
        key=lambda x: (x.split("-")[0], int(x.split("-")[-1]))
        if "-" in x
        else ("z" + x[3], 0),
    )
    instance_types = st.multiselect(
        "select instance type(s)",
        instance_type_options,
        # default=[
        #     instance
        #     for instance in instance_type_options
        #     if instance.startswith("n2d")
        #     # instance
        #     # for instance in instance_type_options
        #     # if "laptop" in instance
        # ],
        default=instance_type_options,
    )
    instance_types = sorted(
        instance_types,
        key=lambda x: (x.split("-")[0], int(x.split("-")[-1]))
        if "-" in x
        else ("z" + x[3], 0),
    )

    sfs = sorted(t.select("sf").distinct().to_pandas()["sf"].tolist())
    scale_factor = st.radio(
        "select scale factor",
        sfs,
        index=sfs.index(sfs[-1]),
    )

    # filetype options
    # filetype_options = sorted(
    #     t.select("file_type").distinct().to_pandas()["file_type"].tolist()
    # )
    # file_type = st.radio(
    #     "select file type",
    #     filetype_options,
    #     index=filetype_options.index("parquet") if "parquet" in filetype_options else 0,
    # )
    file_type = "parquet"

    # query options
    query_numbers = sorted(
        t.select("query_number").distinct().to_pandas()["query_number"].tolist()
    )
    start_query, end_query = st.select_slider(
        "select a range of queries",
        options=query_numbers,
        value=(min(query_numbers), max(query_numbers)),
    )
    query_numbers = list(range(start_query, end_query + 1))

    # log_y
    log_y = st.toggle("log y-axis", True)

    # submit button
    update_button = st.form_submit_button(label="update")

st.markdown("## totals (filtered data)")
totals_metrics(
    t.filter(t["sf"] == scale_factor)
    .filter(t["system"].isin(systems))
    .filter(t["file_type"] == file_type)
    .filter(t["instance_type"].isin(instance_types))
    .filter(t["query_number"] >= start_query)
    .filter(t["query_number"] <= end_query)
)

# aggregate data
agg = (
    t.filter(t["sf"] == scale_factor)
    .filter(t["system"].isin(systems))
    # .filter(t["file_type"].isin(file_type))
    .filter(t["file_type"] == file_type)
    .filter(t["instance_type"].isin(instance_types))
    .filter(t["query_number"] >= start_query)
    .filter(t["query_number"] <= end_query)
    .group_by("system", "instance_type", "sf", "query_number")  # , "file_type")
    .agg(
        mean_execution_seconds=t["execution_seconds"].mean(),
    )
    .order_by(
        ibis.desc("sf"),
        ibis.asc("query_number"),
        ibis.desc("system"),
        ibis.desc("instance_type"),
        ibis.asc("mean_execution_seconds"),
        # ibis.asc("file_type"),
    )
)
agg = agg.join(instance_details, "instance_type")

all_systems = sorted(t.select("system").distinct().to_pandas()["system"].tolist())

category_orders = {
    "query_number": sorted(query_numbers),
    "system": sorted(all_systems),
    "instance_type": instance_types,
}

sfs = agg.select("sf").distinct().to_pandas()["sf"].tolist()
gb_factor = 2 / 5 if file_type == "parquet" else 11 / 10

for sf in sorted(sfs):
    st.markdown("## execution time per query")
    c = px.bar(
        agg,
        x="query_number",
        y="mean_execution_seconds",
        log_y=log_y,
        color="system",
        category_orders=category_orders,
        barmode="group",
        pattern_shape="instance_type",
        hover_data=["cpu_type", "cpu_cores", "memory_gbs"],
        title=f"scale factor: {sf} (~{sf}GB in memory | ~{round(sf * gb_factor, 2)}GB as {file_type})",
    )
    st.plotly_chart(c)

    agg2 = agg.group_by("system", "instance_type").agg(
        present_queries=ibis._["query_number"].collect().unique().sort(),
        total_mean_execution_seconds=ibis._["mean_execution_seconds"].sum(),
    )
    agg2 = agg2.join(instance_details, "instance_type")
    agg2 = (
        agg2.mutate(
            failing_queries=t.filter(t["query_number"].isin(query_numbers))
            .distinct(on="query_number")["query_number"]
            .collect()
            .filter(lambda x: ~agg2["present_queries"].contains(x))
        )
        .mutate(
            num_failing_queries=ibis._["failing_queries"].length(),
            num_successful_queries=ibis._["present_queries"].length(),
        )
        .drop("present_queries")
        .order_by(ibis.desc("memory_gbs"), "system")
    )

    st.markdown("## completed queries by system and instance type")
    c = px.bar(
        agg2,
        x="system",
        y="num_successful_queries",
        color="instance_type",
        barmode="group",
        hover_data=["cpu_type", "cpu_cores", "memory_gbs"],
        category_orders={
            "system": sorted(systems),
            "instance_type": reversed(instance_types),
        },
        title="completed queries",
    )
    st.plotly_chart(c)

    st.markdown("## details by system and instance type")
    st.dataframe(agg2, use_container_width=True)

    st.markdown("---")
