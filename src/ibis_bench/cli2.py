import os
import ibis
import uuid
import typer
import subprocess

from ibis_bench.utils.logging import log
from ibis_bench.utils.monitor import get_raw_json_dir, get_cache_dir

DEFAULT_INSTANCE_TYPE = "unknown"
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
    use_csv: bool = typer.Option(
        False, "--csv", "-c", help="use CSV instead of Parquet"
    ),
    repeat: int = typer.Option(
        3, "--repeat", "-r", help="number of times to repeat the run"
    ),
):
    """
    run bench
    """
    for _ in range(repeat):
        for sf in scale_factors:
            for system in systems:
                for q in queries:
                    cmd = f"bench run {system} -s {sf} -q {q} -i '{instance_type}'"
                    cmd += " --csv" if use_csv else ""

                    log.info(f"running: {cmd}")
                    res = subprocess.run(cmd, shell=True)
                    if res.returncode != 0:
                        log.info(f"failed to run: {cmd}")

                    log.info(f"finished running: {cmd}")


@app.command()
def combine_json(instance_type: str = typer.Argument(..., help="instance type")):
    """
    combine JSON files as Parquet
    """
    file_id = str(uuid.uuid4())

    con = ibis.connect("duckdb://")
    t = con.read_json(f"{get_raw_json_dir()}/file_id=*.json")
    t = t.mutate(instance_type=ibis.literal(instance_type))

    file_path = os.path.join(get_cache_dir(), f"file_id={file_id}.parquet")

    log.info(f"\twriting monitor data to {file_path}...")
    t.to_parquet(file_path)
    log.info(f"\tdone writing monitor data to {file_path}...")


if __name__ == "__main__":
    typer.run(app)
