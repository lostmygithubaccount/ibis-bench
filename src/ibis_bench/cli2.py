import typer
import subprocess

from ibis_bench.utils.logging import log
from ibis_bench.utils.monitor import jsons_to_parquet

DEFAULT_INSTANCE_TYPE = "work laptop"
DEFAULT_SYSTEMS = [
    "ibis-datafusion",
    "ibis-datafusion-sql",
    "ibis-duckdb",
    "ibis-duckdb-sql",
    "ibis-polars",
    "polars-lazy",
]
DEFAULT_SFS = [
    1,
    8,
    16,
    32,
    64,
    128,
]
DEFAULT_QS = range(1, 23)

TYPER_KWARGS = {
    "no_args_is_help": True,
    "add_completion": False,
    "context_settings": {"help_option_names": ["-h", "--help"]},
}

app = typer.Typer(help="ibis-bench", **TYPER_KWARGS)


@app.command()
def run(
    systems: list[str] = typer.Option(
        DEFAULT_SYSTEMS, "--system", "-S", help="system to run on"
    ),
    scale_factors: list[int] = typer.Option(
        DEFAULT_SFS, "--scale-factor", "-s", help="scale factors"
    ),
    queries: list[int] = typer.Option(
        DEFAULT_QS, "--query-number", "-q", help="query numbers"
    ),
    instance_type: str = typer.Option(
        DEFAULT_INSTANCE_TYPE, "--instance-type", "-i", help="instance type"
    ),
):
    """
    default run all
    """
    for sf in scale_factors:
        for system in systems:
            for q in queries:
                cmd = f"bench run {system} -s {sf} -q {q} -i '{instance_type}'"

                log.info(f"running: {cmd}")
                res = subprocess.run(cmd, shell=True)
                if res.returncode != 0:
                    log.info(f"failed to run: {cmd}")

                log.info(f"finished running: {cmd}")


@app.command()
def combine_json():
    """
    combine JSON files as Parquet
    """
    jsons_to_parquet()


if __name__ == "__main__":
    typer.run(app)
