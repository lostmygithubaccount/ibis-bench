import ibis
import gcsfs
import tomllib
import streamlit as st
import plotly.express as px

from ibis_bench.utils.monitor import get_timings_dir, get_cache_dir, get_raw_json_dir  # noqa

with open("pyproject.toml", "rb") as f:
    pyproject = tomllib.load(f)

dependencies = pyproject["project"]["dependencies"]

datafusion_version = [d for d in dependencies if "datafusion" in d][0]
duckdb_version = [d for d in dependencies if "duckdb" in d][0]
polars_version = [d for d in dependencies if "polars" in d][0]
ibis_version = [d for d in dependencies if "ibis" in d][0]

st.set_page_config(layout="wide")
st.title("WIP Ibis benchmarking")
details = f"""
work in progress...

versions:

- DataFusion: `{datafusion_version}`
- DuckDB: `{duckdb_version}`
- Polars: `{polars_version}`
- Ibis: `{ibis_version}`

**DATA IS NOT FINALIZED**

the purpose of this dashboard is to compare TPC-H benchmarks across the big three single-node, Apache Arrow-based, modern OLAP engines: DuckDB, DataFusion, and Polars
"""
details = details.strip()
st.markdown(details)

ibis.options.interactive = True
ibis.options.repr.interactive.max_rows = 20
ibis.options.repr.interactive.max_columns = None

# dark mode for px
px.defaults.template = "plotly_dark"

# instance type mapping

gcp_vm_cpu_map = {
    "c3": "Intel Saphire Rapids",
    "n2": "Intel Cascade and Ice Lake",
    "n2d": "AMD EPYC",
}
gcp_vm_types = ["standard"]
gcp_vm_endings = [2, 4, 8, 16, 22, 32, 44]
instance_type_details = {
    "work laptop": {
        "type": "MacBook Pro (16-inch, 2021)",
        "cpu": "Apple M1 Max",
        "cpu_cores": "10",
        "ram": "32GB",
        "disk": "1000GB SSD",
    },
    "personal laptop": {
        "type": "MacBook Pro (16-inch, 2023)",
        "cpu": "Apple M2 Max",
        "cpu_cores": "12",
        "ram": "96GB",
        "disk": "2000GB SSD",
    },
}

for gcp_vm_cpu in gcp_vm_cpu_map.keys():
    for gcp_vm_type in gcp_vm_types:
        for gcp_vm_ending in gcp_vm_endings:
            instance_type_details[f"{gcp_vm_cpu}-{gcp_vm_type}-{gcp_vm_ending}"] = {
                "type": "GCP VM instance",
                "cpu": f"{gcp_vm_cpu_map[gcp_vm_cpu]}",
                "cpu_cores": f"{gcp_vm_ending}",
                "ram": f"{gcp_vm_ending * 4}GB",
                "disk": "1000 GB SSD",
            }


def get_t():
    # ibis connection
    con = ibis.connect("duckdb://cache.ddb")
    # con = ibis.connect("duckdb://")

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
    table_name = "bench_data"
    if table_name not in con.list_tables():
        t = (
            con.read_parquet(glob)
            .mutate(
                timestamp=ibis._["timestamp"].cast("timestamp"),
            )
            .drop("file_id")
            .distinct()  # TODO: hmmmmmm
            .filter(ibis._["file_type"] == "parquet")  # TODO: remove after CSV runs
            .cache()
        )
        con.create_table(table_name, t)
    else:
        t = con.table(table_name)

    return t


t = get_t()


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
        default=system_options,
    )

    # sfs options
    sfs = sorted(t.select("sf").distinct().to_pandas()["sf"].tolist())
    scale_factors = st.multiselect(
        "select scale factor(s)",
        sfs,
        default=sfs,
    )

    instance_type_options = sorted(
        t.select("instance_type").distinct().to_pandas()["instance_type"].tolist(),
        key=lambda x: (x.split("-")[0], int(x.split("-")[-1])) if "-" in x else (x, 0),
    )
    instance_types = st.multiselect(
        "select instance type(s)",
        instance_type_options,
        default=[
            instance
            for instance in instance_type_options
            if instance.startswith("n2d")
            # instance
            # for instance in instance_type_options
            # if "laptop" in instance
        ],
        # default=instance_type_options,
    )
    instance_types = sorted(
        instance_types,
        key=lambda x: (x.split("-")[0], int(x.split("-")[-1])) if "-" in x else (x, 0),
    )

    # filetype options
    filetype_options = sorted(
        t.select("file_type").distinct().to_pandas()["file_type"].tolist()
    )
    file_type = st.radio(
        "select file type",
        filetype_options,
        index=filetype_options.index("parquet") if "parquet" in filetype_options else 0,
    )

    # query options
    query_numbers = sorted(
        t.select("query_number").distinct().to_pandas()["query_number"].tolist()
    )
    start_query, end_query = st.select_slider(
        "select a range of queries",
        options=query_numbers,
        value=(min(query_numbers), max(query_numbers)),
    )

    # submit button
    update_button = st.form_submit_button(label="update")

st.markdown("## totals (filtered data)")
totals_metrics(
    t.filter(t["sf"].isin(scale_factors))
    .filter(t["system"].isin(systems))
    .filter(t["file_type"] == file_type)
    .filter(t["instance_type"].isin(instance_types))
    .filter(t["query_number"] >= start_query)
    .filter(t["query_number"] <= end_query)
)

# display instance type details
st.markdown("## instance type details")

tabs = st.tabs(instance_types)
for instance_type in instance_types:
    with tabs[instance_types.index(instance_type)]:
        st.markdown(f"### {instance_type}")
        for k, v in instance_type_details[instance_type].items():
            st.write(f"{k}: {v}")

# aggregate data
agg = (
    t.filter(t["sf"] >= 1)
    .filter(t["sf"].isin(scale_factors))
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

all_systems = sorted(t.select("system").distinct().to_pandas()["system"].tolist())

sfs = agg.select("sf").distinct().to_pandas()["sf"].tolist()
category_orders = {
    "query_number": sorted(query_numbers),
    "system": sorted(all_systems),
    "instance_type": instance_types,
}

gb_factor = 2 / 5 if file_type == "parquet" else 11 / 10

for sf in sorted(sfs):
    st.markdown(f"## scale factor: {sf}")
    c = px.bar(
        agg.filter(agg["sf"] == sf),
        x="query_number",
        y="mean_execution_seconds",
        color="system",
        category_orders=category_orders,
        barmode="group",
        pattern_shape="instance_type",
        title=f"scale factor: {sf} (~{sf}GB in memory | ~{round(sf * gb_factor, 2)}GB as {file_type})",
    )
    st.plotly_chart(c)

    all_systems = sorted(
        agg.filter(agg["sf"] == sf)
        .select("system")
        .distinct()
        .to_pandas()["system"]
        .tolist()
    )
    all_queries = range(start_query, end_query + 1)

    tabs = st.tabs(instance_types)

    for i in range(len(tabs)):
        with tabs[i]:
            cols = st.columns(len(all_systems))
            for system in sorted(
                agg.filter(agg["sf"] == sf)
                .filter(agg["instance_type"] == instance_types[i])
                .select("system")
                .distinct()
                .to_pandas()["system"]
                .tolist()
            ):
                queries_completed = (
                    agg.filter(agg["sf"] == sf)
                    .filter(agg["system"] == system)
                    .filter(agg["instance_type"] == instance_types[i])
                    .select("query_number")
                    .distinct()
                    .to_pandas()["query_number"]
                    .tolist()
                )

                missing_queries = sorted(
                    list(set(all_queries) - set(queries_completed))
                )

                total_runtime_seconds = (
                    agg.filter(agg["sf"] == sf)
                    .filter(agg["system"] == system)
                    .filter(agg["instance_type"] == instance_types[i])[
                        "mean_execution_seconds"
                    ]
                    .sum()
                    .to_pandas()
                )

                with cols[all_systems.index(system)]:
                    st.metric(
                        label=f"{system} queries completed",
                        value=f"{len(queries_completed)}/{len(all_queries)}",
                    )
                    st.metric(
                        label=f"{system} total runtime seconds",
                        value=round(
                            total_runtime_seconds,
                            2,
                        ),
                    )
                    st.metric(
                        label=f"{system} queries missing",
                        value="\n".join([str(q) for q in missing_queries]),
                        help="\n".join([str(q) for q in missing_queries]),
                    )

    st.markdown("---")
