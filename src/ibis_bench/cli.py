import ibis
import uuid
import typer
import subprocess

from ibis_bench.utils.logging import log
from ibis_bench.utils.monitor import monitor_it, jsons_to_parquet
from ibis_bench.utils.gen_data import generate_data
from ibis_bench.utils.read_data import get_ibis_tables, get_polars_tables

DEFAULT_SCALE_FACTORS = [1]
DEFAULT_N_PARTITIONS = [1]

TYPER_KWARGS = {
    "no_args_is_help": True,
    "add_completion": False,
    "context_settings": {"help_option_names": ["-h", "--help"]},
}

app = typer.Typer(help="ibis-bench", **TYPER_KWARGS)


@app.command()
def gen_data(
    scale_factor: list[int] = typer.Option(
        DEFAULT_SCALE_FACTORS, "--scale-factor", "-s", help="scale factors"
    ),
    n_partitions: list[int] = typer.Option(
        DEFAULT_N_PARTITIONS, "--n-partitions", "-n", help="number of partitions"
    ),
    csv: bool = typer.Option(
        False, "--csv", "-c", help="generate CSV files in addition to Parquet"
    ),
):
    """
    generate tpc-h benchmarking data
    """
    for sf in sorted(scale_factor):
        for n in sorted(n_partitions):
            generate_data(sf, n, csv=csv)


@app.command()
def run(
    systems: list[str] = typer.Argument(..., help="system to run on"),
    scale_factor: list[int] = typer.Option(
        DEFAULT_SCALE_FACTORS, "--scale-factor", "-s", help="scale factors"
    ),
    n_partitions: list[int] = typer.Option(
        DEFAULT_N_PARTITIONS, "--n-partitions", "-n", help="number of partitions"
    ),
    q_number: list[int] = typer.Option(
        None, "--query-number", "-q", help="query numbers"
    ),
    exclude_queries: list[int] = typer.Option(
        None, "--exclude-queries", "-e", help="exclude query numbers"
    ),
    use_csv: bool = typer.Option(
        False, "--csv", "-c", help="use CSV files instead of parquet"
    ),
    instance_type: str = typer.Option(
        None, "--instance-type", "-i", help="instance type"
    ),
):
    """
    run tpc-h benchmarking queries
    """
    session_id = str(uuid.uuid4())

    for sf in sorted(scale_factor):
        for n in sorted(n_partitions):
            for system in systems:
                system_parts = system.split("-")

                if system_parts[0] == "ibis" and system_parts[-1] != "sql":
                    backend = system_parts[1]
                    con = ibis.connect(f"{backend}://")
                    log.info(f"connected to {backend} backend")

                    from ibis_bench.queries.ibis import all_queries

                    (
                        customer,
                        lineitem,
                        nation,
                        orders,
                        part,
                        partsupp,
                        region,
                        supplier,
                    ) = get_ibis_tables(sf=sf, n_partitions=n, con=con, csv=use_csv)
                elif system_parts[0] == "ibis" and system_parts[-1] == "sql":
                    backend = system_parts[1]
                    con = ibis.connect(f"{backend}://")
                    log.info(f"connected to {backend} backend (for SQL)")

                    from ibis_bench.queries.sql import all_queries

                    (
                        customer,
                        lineitem,
                        nation,
                        orders,
                        part,
                        partsupp,
                        region,
                        supplier,
                    ) = get_ibis_tables(sf=sf, n_partitions=n, con=con, csv=use_csv)
                elif system_parts[0] == "polars":
                    backend = system_parts[0]
                    lazy = system_parts[1] == "lazy"
                    log.info(f"using Polars with lazy={lazy}")

                    from ibis_bench.queries.polars import all_queries

                    (
                        customer,
                        lineitem,
                        nation,
                        orders,
                        part,
                        partsupp,
                        region,
                        supplier,
                    ) = get_polars_tables(sf=sf, n_partitions=n, lazy=lazy, csv=use_csv)

                queries = {
                    q: query
                    for q, query in enumerate(all_queries, start=1)
                    if (q_number is None or q in q_number)
                    and (exclude_queries is None or q not in exclude_queries)
                }

                for q, query in queries.items():
                    try:
                        monitor_it(
                            query,
                            sf=sf,
                            n_partitions=n,
                            query_number=q,
                            system=system,
                            session_id=session_id,
                            instance_type=instance_type,
                            use_csv=use_csv,
                            # tpch tables
                            customer=customer,
                            lineitem=lineitem,
                            nation=nation,
                            orders=orders,
                            part=part,
                            partsupp=partsupp,
                            region=region,
                            supplier=supplier,
                            # used for SQL runs
                            dialect=backend if backend else None,
                        )
                    except Exception as e:
                        log.info(
                            f"error running query {q} at scale factor {sf} and {n} partitions: {e}"
                        )


# TODO: hmmmm
@app.command()
def run_all():
    """
    default run all
    """
    instance_type = "work laptop"
    all_systems = [
        "ibis-duckdb",
        "ibis-duckdb-sql",
        "ibis-datafusion",
        "ibis-datafusion-sql",
        "polars-lazy",
        "ibis-polars",
    ]
    all_sfs = [
        1,
        8,
        16,
        32,
        64,
        128,
    ]
    all_qs = range(1, 23)

    for sf in all_sfs:
        for system in all_systems:
            for q in all_qs:
                cmd = f"bench run {system} -s {sf} -q {q} -i '{instance_type}'"

                log.info(f"running: {cmd}")
                res = subprocess.run(cmd, shell=True)
                if res.returncode != 0:
                    log.info(f"failed to run: {cmd}")

                log.info(f"finished running: {cmd}")


@app.command()
def cache_json():
    """
    cache JSON files as Parquet
    """
    jsons_to_parquet()


if __name__ == "__main__":
    typer.run(app)
