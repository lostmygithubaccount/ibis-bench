# imports
import ibis
import uuid
import typer

from ibis_bench.utils.logging import log
from ibis_bench.utils.monitor import monitor_it

## import tpch functions
from ibis_bench.tpch.utils.gen_data import generate_data as tpch_generate_data
from ibis_bench.tpch.utils.read_data import (
    get_ibis_tables as tpch_get_ibis_tables,
    get_polars_tables as tpch_get_polars_tables,
)

## import tpcds functions
from ibis_bench.tpcds.utils.gen_data import generate_data as tpcds_generate_data
# from ibis_bench.tpcds.utils.read_data import (
#     get_ibis_tables as tpcds_get_ibis_tables,
#     get_polars_tables as tpcds_get_polars_tables,
# )

# defaults
DEFAULT_SCALE_FACTORS = [1]
DEFAULT_N_PARTITIONS = [1]

TYPER_KWARGS = {
    "no_args_is_help": True,
    "add_completion": False,
    "context_settings": {"help_option_names": ["-h", "--help"]},
}

# typer apps
app = typer.Typer(help="ibis-bench", **TYPER_KWARGS)
tpch_app = typer.Typer(help="TPC-H commands", **TYPER_KWARGS)
tpcds_app = typer.Typer(help="TPC-DS commands", **TYPER_KWARGS)

## add subcommands
app.add_typer(tpch_app, name="tpch")
app.add_typer(tpcds_app, name="tpcds")

## add subcommand aliases
app.add_typer(tpch_app, name="h", hidden=True)
app.add_typer(tpcds_app, name="ds", hidden=True)


# TPC-H commands
@tpch_app.command("gen")
def tpch_gen_data(
    scale_factors: list[int] = typer.Option(
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
    for sf in sorted(scale_factors):
        for n in sorted(n_partitions):
            tpch_generate_data(sf, n, csv=csv)


@tpch_app.command("run")
def tpch_run(
    systems: list[str] = typer.Argument(..., help="system to run on"),
    scale_factors: list[int] = typer.Option(
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
    decimal_to_float: bool = typer.Option(
        False, "--decimal-to-float", "-d", help="convert decimal to float"
    ),
    instance_type: str = typer.Option(
        None, "--instance-type", "-i", help="instance type"
    ),
):
    """
    run tpc-h benchmarking queries
    """
    session_id = str(uuid.uuid4())

    for sf in sorted(scale_factors):
        for n in sorted(n_partitions):
            for system in systems:
                system_parts = system.split("-")

                if system_parts[0] == "ibis" and system_parts[-1] != "sql":
                    backend = system_parts[1]
                    con = ibis.connect(f"{backend}://")
                    log.info(f"connected to {backend} backend")

                    from ibis_bench.tpch.queries.ibis import all_queries

                    (
                        customer,
                        lineitem,
                        nation,
                        orders,
                        part,
                        partsupp,
                        region,
                        supplier,
                    ) = tpch_get_ibis_tables(
                        sf=sf,
                        n_partitions=n,
                        con=con,
                        csv=use_csv,
                        decimal_to_float=decimal_to_float,
                    )
                elif system_parts[0] == "ibis" and system_parts[-1] == "sql":
                    backend = system_parts[1]
                    con = ibis.connect(f"{backend}://")
                    log.info(f"connected to {backend} backend (for SQL)")

                    from ibis_bench.tpch.queries.sql import all_queries

                    (
                        customer,
                        lineitem,
                        nation,
                        orders,
                        part,
                        partsupp,
                        region,
                        supplier,
                    ) = tpch_get_ibis_tables(
                        sf=sf,
                        n_partitions=n,
                        con=con,
                        csv=use_csv,
                        decimal_to_float=decimal_to_float,
                    )
                elif system_parts[0] == "polars":
                    backend = system_parts[0]
                    lazy = system_parts[1] == "lazy"
                    log.info(f"using Polars with lazy={lazy}")

                    from ibis_bench.tpch.queries.polars import all_queries

                    (
                        customer,
                        lineitem,
                        nation,
                        orders,
                        part,
                        partsupp,
                        region,
                        supplier,
                    ) = tpch_get_polars_tables(
                        sf=sf,
                        n_partitions=n,
                        lazy=lazy,
                        csv=use_csv,
                        decimal_to_float=decimal_to_float,
                    )

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
                            decimal_to_float=decimal_to_float,
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


# TPC-DS commands
@tpcds_app.command("gen")
def tpcds_gen_data(
    scale_factors: list[int] = typer.Option(
        DEFAULT_SCALE_FACTORS, "--scale-factor", "-s", help="scale factors"
    ),
    csv: bool = typer.Option(
        False, "--csv", "-c", help="generate CSV files in addition to Parquet"
    ),
):
    """
    generate tpc-h benchmarking data
    """
    for sf in sorted(scale_factors):
        tpcds_generate_data(sf, csv=csv)


if __name__ == "__main__":
    typer.run(app)
