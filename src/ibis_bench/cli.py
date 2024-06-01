import ibis
import uuid
import typer
import polars as pl
import warnings


from ibis_bench.utils.monitor import monitor_it
from ibis_bench.utils.gen_data import generate_data
from ibis_bench.utils.read_data import get_ibis_tables, get_polars_tables

warnings.filterwarnings("ignore")  # YOLO

default_kwargs = {
    "no_args_is_help": True,
    "add_completion": False,
    "context_settings": {"help_option_names": ["-h", "--help"]},
}


app = typer.Typer(help="ibis-bench", **default_kwargs)


@app.command()
def hello():
    """
    say hello
    """
    print("hello")


@app.command()
def there():
    """
    say there
    """
    print("there")


@app.command()
def gen_data(
    sf: int = typer.Argument(1, help="scale factor"),
    n_partitions: int = typer.Option(
        1, "--n-partitions", "-n", help="number of partitions"
    ),
    sfs: list[int] = typer.Option(None, "--scale-factors", "-S", help="scale factors"),
    n_partitionses: list[int] = typer.Option(
        None, "--n-partitiones", "-N", help="number of partitions"
    ),
):
    """
    generate tpc-h benchmarking data
    """
    sfs = [sf] if sfs is None else sfs
    ns = [n_partitions] if n_partitionses is None else n_partitionses

    for sf in sorted(sfs):
        for n in sorted(ns):
            generate_data(sf, n)


@app.command()
def run(
    system: str = typer.Argument(..., help="system to run on"),
    sf: int = typer.Option(1, "--scale-factor", "-s", help="scale factor"),
    n_partitions: int = typer.Option(
        1, "--n-partitions", "-n", help="number of partitions"
    ),
    sfs: list[int] = typer.Option(None, "--scale-factors", "-S", help="scale factors"),
    n_partitionses: list[int] = typer.Option(
        None, "--n-partitiones", "-N", help="number of partitions"
    ),
    q_number: int = typer.Option(1, "--query-number", "-q", help="query number"),
    all_queries: bool = typer.Option(
        False, "--all-queries", "-A", help="run all queries"
    ),
):
    """
    run tpc-h benchmarking queries
    """

    session_id = str(uuid.uuid4())

    sfs = [sf] if sfs is None else sfs
    ns = [n_partitions] if n_partitionses is None else n_partitionses

    for sf in sorted(sfs):
        for n in sorted(ns):
            system_parts = system.split("-")

            if system_parts[0] == "ibis":
                backend = system_parts[1]
                con = ibis.connect(f"{backend}://")

                from ibis_bench.queries.ibis import q1, q2, all_queries

                # need to map the query number to the function
                # e.g. 1 -> q1()
                queries = all_queries if all_queries else [globals()[f"q{q_number}"]]

                customer, lineitem, nation, orders, part, partsupp, region, supplier = (
                    get_ibis_tables(sf=sf, n_partitions=n_partitions, con=con)
                )

                for q_number, query in enumerate(queries, start=1):
                    monitor_it(
                        query,
                        sf=sf,
                        n_partitions=n_partitions,
                        query_number=q_number,
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

            elif system_parts[0] == "polars":
                lazy = system_parts[1] == "lazy"

                from ibis_bench.queries.polars import q1, q2, all_queries

                queries = all_queries if all_queries else [globals()[f"q{q_number}"]]

                customer, lineitem, nation, orders, part, partsupp, region, supplier = (
                    get_polars_tables(sf=sf, n_partitions=n_partitions, lazy=lazy)
                )

                for q_number, query in enumerate(queries, start=1):
                    monitor_it(
                        query,
                        sf=sf,
                        n_partitions=n_partitions,
                        query_number=q_number,
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


if __name__ == "__main__":
    typer.run(app)
