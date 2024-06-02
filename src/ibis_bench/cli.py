import ibis
import uuid
import typer
import warnings

from ibis_bench.utils.monitor import monitor_it
from ibis_bench.utils.gen_data import generate_data
from ibis_bench.utils.read_data import get_ibis_tables, get_polars_tables

warnings.filterwarnings("ignore")  # YOLO

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
):
    """
    generate tpc-h benchmarking data
    """
    for sf in sorted(scale_factor):
        for n in sorted(n_partitions):
            generate_data(sf, n)


@app.command()
def run(
    system: str = typer.Argument(..., help="system to run on"),
    scale_factor: list[int] = typer.Option(
        DEFAULT_SCALE_FACTORS, "--scale-factor", "-s", help="scale factors"
    ),
    n_partitions: list[int] = typer.Option(
        DEFAULT_N_PARTITIONS, "--n-partitions", "-n", help="number of partitions"
    ),
    q_number: list[int] = typer.Option(
        None, "--query-number", "-q", help="query numbers"
    ),
):
    """
    run tpc-h benchmarking queries
    """

    session_id = str(uuid.uuid4())

    for sf in sorted(scale_factor):
        for n in sorted(n_partitions):
            system_parts = system.split("-")

            if system_parts[0] == "ibis":
                backend = system_parts[1]
                con = ibis.connect(f"{backend}://")

                from ibis_bench.queries.ibis import all_queries

                customer, lineitem, nation, orders, part, partsupp, region, supplier = (
                    get_ibis_tables(sf=sf, n_partitions=n, con=con)
                )
            elif system_parts[0] == "polars":
                lazy = system_parts[1] == "lazy"

                from ibis_bench.queries.polars import all_queries

                customer, lineitem, nation, orders, part, partsupp, region, supplier = (
                    get_polars_tables(sf=sf, n_partitions=n, lazy=lazy)
                )

            queries = {
                q: query
                for q, query in enumerate(all_queries, start=1)
                if q_number is None or q in q_number
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
                        customer=customer,
                        lineitem=lineitem,
                        nation=nation,
                        orders=orders,
                        part=part,
                        partsupp=partsupp,
                        region=region,
                        supplier=supplier,
                    )
                except Exception as e:
                    print(
                        f"error running query {q} at scale factor {sf} and {n_partitions} partitions: {e}"
                    )


if __name__ == "__main__":
    typer.run(app)
