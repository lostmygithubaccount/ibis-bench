import streamlit as st

st.set_page_config(layout="wide")
st.title("Ibis benchmarking methodology")

methodology = """
See the [accompanying blog post](https://ibis-project.org/posts/ibis-bench) for more information.

## System naming convention

For convenience, we define a hyphen-separated naming convention for the 'systems' we run on:

- `ibis-*`: Ibis with various backends
    - `ibis-<backend>`: Ibis dataframe code running on the specified backend
    - `ibis-<backend>-sql`: Ibis running SQL code directly on the specified backend
- `polars-*`: Polars with various configurations
    - `polars-lazy`: Polars via `pl.LazyFrame`s
"""
st.markdown(methodology)

methodology = """
## Important considerations

Noted in more detail below (with code as the source of truth), but some important considerations include:

- TPC-H data is generated via DuckDB as Parquet files, with non-integers as decimals
- data is read in with decimals converted to floats for each query, by Ibis (via the corresponding backend) and Polars
    - additionally, hive-style partitioned columns are dropped
- queries are run 3 times and the average is taken
- all data is available in a public `gs://ibis-bench/` bucket for analysis (you can take a minimum instead of an average, for example)
- some Polars TPC-H queries were recently re-written; we will update `ibis-bench` and re-run soon including these and using newer versions of libraries
"""
st.markdown(methodology)

methodology = """
## Data generation

We use the [DuckDB TPC-H Extension](https://duckdb.org/docs/extensions/tpch.html) to generate the TPC-H data as Parquet, and optionally CSV files. The data can be partitioned.

For the current results, only Parquet data at `n=1` partitions at various scale factors are used. After generation, the data is uploaded to a cloud bucket for re-use (see the `justfile` below).

You can see the data generation source code below:
"""
st.markdown(methodology)

with open("src/ibis_bench/utils/gen_data.py") as f:
    code = f.read()

with st.expander("Show data generation code", expanded=False):
    st.code(code, line_numbers=True, language="python")

methodology = """
## Data ingestion

Data is ingested in two ways:

1. Using Ibis
2. Using Polars

All 'systems' read via Ibis **except for `polars-*`, e.g. `polars-lazy`**. 

**Important notes**:
- decimals can be converted to floats (and were for the current results)
- hive-style partitioned columns are dropped

You can see the data reading source code below:
"""
st.markdown(methodology)

with open("src/ibis_bench/utils/read_data.py") as f:
    code = f.read()

with st.expander("Show data reading code", expanded=False):
    st.code(code, line_numbers=True, language="python")

methodology = """
## Query definition

Each TPC-H query is written as a standalone Python function of the form `q<N>`, where `<N>` is the query number.

We define three sets of queries:

1. Ibis dataframe code
2. Ibis SQL code
3. Polars dataframe code

The first 10 Ibis dataframe queries were translated from the first 10 Polars queries. The remaining 12 were copied from existing Ibis TPC-H queries. The Ibis SQL queries were also directly copied from existing Ibis TPC-H SQL query code. The code was lightly modified so that functions are self-contained and for legibility when reading across systems.

**Important notes**:
- it is difficult to fairly translate SQL to dataframe code
- Polars is lacking some features (e.g. subqueries) and is in the process of re-writing some TPC-H queries
- SQL queries are transpiled to the backend's SQL dialect through Ibis (via SQLGlot)

You can see the query definition source code below:
"""
st.markdown(methodology)

with open("src/ibis_bench/queries/ibis.py") as f:
    code = f.read()

with st.expander("Show Ibis query code", expanded=False):
    st.code(code, line_numbers=True, language="python")

with open("src/ibis_bench/queries/polars.py") as f:
    code = f.read()

with st.expander("Show Polars query code", expanded=False):
    st.code(code, line_numbers=True, language="python")

with open("src/ibis_bench/queries/sql.py") as f:
    code = f.read()

with st.expander("Show Ibis SQL query code", expanded=False):
    st.code(code, line_numbers=True, language="python")

methodology = """
## Measuring query execution time

Ibis is lazyily evaluated, requiring some call by the user to trigger computation. These include methods like:

- `table.to_pandas()`
- `table.to_pyarrow()`
- `table.to_csv()`
- `table.to_parquet()`
- ...

You can also trigger computation by turning on interactive mode and outputting a table, however this may add a `LIMIT` clause to the query and thus wouldn't be a fair comparison.

Polars is eager by default, but it's generally recommended to use lazy dataframes for performance reasons. Then, you also need to call a method like:

- `.collect()`
- `.sink_parquet()`
- ...

to trigger computation. For this benchmark, we're only evaluating lazy dataframes. For consistency, **we write the results of each query to a Parquet file**. This forces computation and avoids any differences in the output.

### We are measuring...

First, we read in all 8 TPC-H tables via the corresponding lazy Parquet method. This is effectively instant, as for Ibis and Polars no data is read at this time. Then, we get a `start_time = time.time()` and run something like `q(lineitem, orders, ...).to_parquet("results/q.parquet")`. Finally, we get an `end_time = time.time()` and calculate the difference. The actual code differs (see below) but this is the general idea of what we're measuring and how.
"""
st.markdown(methodology)

with open("src/ibis_bench/utils/write_data.py") as f:
    code = f.read()

with st.expander("Show data writing code", expanded=False):
    st.code(code, line_numbers=True, language="python")

with open("src/ibis_bench/utils/monitor.py") as f:
    code = f.read()

with st.expander("Show monitoring code", expanded=False):
    st.code(code, line_numbers=True, language="python")

methodology = """
## Running the benchmark

To run the benchmark, I wanted a CLI helper tool and ended up making two. The first is used to generate the TPC-H data and run the queries. The second mainly functions as a wrapper around the first to run it in a subprocess. This is because queries can and do fail by being killed by the operating system, generally when memory pressure is too high. To avoid stopping the benchmark, we run each query in a subprocess and continue on failures.

The second CLI also has a tool for combining the JSON timings data for each query into a single Parquet file. This is useful because reading thousands of files from a cloud bucket is very slow.

Currently, each query is run 3 times. In visualizing, the average is taken.

### The `bench` CLI tool

The `bench` tool has a `gen-data` command and a `run` command that work across different systems, scale factors, partitions, and more.
"""
st.markdown(methodology)

with open("src/ibis_bench/cli.py") as f:
    code = f.read()

with st.expander("Show `bench` code", expanded=False):
    st.code(code, line_numbers=True, language="python")

methodology = """
### The `bench2` CLI tool

The `bench2` tool has a `combine-json` command and a `run` command.

"""
st.markdown(methodology)

with open("src/ibis_bench/cli2.py") as f:
    code = f.read()

with st.expander("Show `bench2` code", expanded=False):
    st.code(code, line_numbers=True, language="python")

methodology = """
## Running on the cloud

Most runs are done on GCP VMs. The `justfile` in the repo contains common commands that are used throughout, especially for cloud things. An outline to repeat the process follows.

Edit the `justfile` to the desired VM settings (name, type, region).

Create the VM:

```bash
just create-vm
```

SSH into the VM:

```bash
just vm-ssh
```

Clone and change into the `ibis-bench` repo:

```bash
git clone https://github.com/lostmygithubaccount/ibis-bench
cd ibis-bench
```

Run the VM bootstrap script:

```bash
bash vm-bootstrap.sh
```

This:

- installs `just`
- installs `python3.11`
- installs and sets up some other useful things

Source the `~/.bashrc` file for changes to take effect:

```bash
source ~/.bashrc
```

Now install `ibis-bench`:

```bash
just setup
```

Download the TPC-H data:

```bash
just tpch-download-parquet
```

And run the benchmark:

```bash
just run-all-parquet
```

This will run the process in the background and continue running even if the SSH connection is lost. Logs are written to `out.log` and you can also use `top` to check that `bench` is running.

Once the benchmark is complete, combine the JSON files:

```bash
bench2 combine-json "<INSTANCE_NAME>"
```

This is done manually to avoid mistakes with the `justfile`.

Then authenticate with GCP so we can upload the logs to the bucket:

```bash
gcloud auth login
```

Upload the logs:

```bash
just logs-upload
```

Exit from the VM and delete it:
    
```bash
exit
just vm-delete
```

And that's it! The logs are now in the bucket and can be analyzed.
"""
st.markdown(methodology)


with open("justfile") as f:
    code = f.read()

with st.expander("Show Justfile", expanded=False):
    st.code(code, line_numbers=True, language="justfile")

with open("vm-bootstrap.sh") as f:
    code = f.read()

with st.expander("Show VM bootstrap script", expanded=False):
    st.code(code, line_numbers=True, language="bash")

methodology = """
## Results

Results are publicly available in the `gs://ibis-bench/` bucket. The `bench_logs_v2` directory contains current results.
"""
st.markdown(methodology)
