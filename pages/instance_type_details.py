import ibis
import streamlit as st

from results import get_t

#st.set_page_config(layout="wide")
st.title("Ibis benchmarking instance type details")

t = get_t()

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

# st.dataframe(instance_details, use_container_width=True)
st.table(instance_details)
