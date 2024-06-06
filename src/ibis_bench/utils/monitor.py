import os
import ibis
import json
import time
import uuid
import psutil
import tracemalloc

from datetime import datetime

from ibis_bench.utils.logging import log
from ibis_bench.utils.write_data import write_results


def get_timings_dir():
    dir_name = "bench_logs_v1"
    # dir_name = "bench_logs_temp"

    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    return dir_name


def get_raw_json_dir():
    dir_name = os.path.join(get_timings_dir(), "raw_json")

    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    return dir_name


def get_cache_dir():
    dir_name = os.path.join(get_timings_dir(), "cache")

    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    return dir_name


def monitor_it(
    func,
    sf: int,
    n_partitions: int,
    query_number: int,
    system: str,
    session_id: str,
    instance_type: str,
    use_csv: bool,
    *args,
    **kwargs,
):
    log.info(
        f"running and monitoring for system {system} query {query_number} at scale factor {sf} and {n_partitions} partitions (session id {session_id})..."
    )

    process = psutil.Process()

    # Start tracing memory allocations
    tracemalloc.start()

    # Calculate CPU usage before and after running the function
    cpu_usage_start = process.cpu_percent(interval=None)
    start_time = time.time()

    write_results(func(*args, **kwargs), sf, n_partitions, system, query_number)
    log.info(
        f"done running and monitoring for system {system} query {query_number} at scale factor {sf} and {n_partitions} partitions (session id {session_id})..."
    )

    elapsed_time = time.time() - start_time
    cpu_usage_end = process.cpu_percent(interval=None)

    # Get memory usage data
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    peak_cpu = (cpu_usage_end - cpu_usage_start) / (elapsed_time / 100)

    data = {
        "session_id": session_id,
        "instance_type": instance_type,
        "system": system,
        "timestamp": datetime.utcnow().isoformat(),
        "sf": sf,
        "n_partitions": n_partitions,
        "query_number": query_number,
        "execution_seconds": elapsed_time,
        "file_type": "csv" if use_csv else "parquet",
        "peak_cpu": peak_cpu,
        "peak_memory": peak / 1024**3,
    }

    write_monitor_results(data)


def write_monitor_results(results):
    file_id = str(uuid.uuid4())
    file_path = os.path.join(get_raw_json_dir(), f"file_id={file_id}.json")

    log.info(f"\twriting monitor data to {file_path}...")
    with open(file_path, "w") as f:
        json.dump(results, f)
    log.info(f"\tdone writing monitor data to {file_path}...")


def jsons_to_parquet():
    file_id = str(uuid.uuid4())

    con = ibis.connect("duckdb://")
    t = con.read_json(f"{get_raw_json_dir()}/file_id=*.json")

    file_path = os.path.join(get_cache_dir(), f"file_id={file_id}.parquet")

    log.info(f"\twriting monitor data to {file_path}...")
    t.to_parquet(file_path)
    log.info(f"\tdone writing monitor data to {file_path}...")
