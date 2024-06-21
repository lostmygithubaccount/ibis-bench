import sys
import streamlit as st

sys.path.append("..")

from results import get_t, get_instance_details

# st.set_page_config(layout="wide")
st.title("Ibis benchmarking instance type details")

t = get_t()
instance_details = get_instance_details(t)

# st.dataframe(instance_details, use_container_width=True)
st.table(instance_details)
