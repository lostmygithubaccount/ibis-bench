import os
import json
import time
import uuid
import gcsfs
import psutil
import tracemalloc

from datetime import datetime

from ibis_bench.utils.logging import log
from ibis_bench.utils.write_data import write_results


def get_timings_dir():
    dir_name = "benchy_logs_v10"
    # dir_name = "bench_logs_temp"

    return dir_name


def monitor_it(
    func,
    sf: int,
    n_partitions: int,
    query_number: int,
    system: str,
    session_id: str,
    cloud_logging: bool,
    instance_type: str,
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
        "peak_cpu": peak_cpu,
        "peak_memory": peak / 1024**3,
    }

    write_monitor_results(data, cloud_logging=cloud_logging)


def write_monitor_results(results, cloud_logging=False):
    file_id = str(uuid.uuid4())
    dir_name = get_timings_dir()

    if cloud_logging:
        fs = gcsfs.GCSFileSystem()
        file_path = f"gs://ibis-benchy/{dir_name}/{file_id}.json"
        open_fn = fs.open
    else:
        if not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
        file_path = f"{dir_name}/{file_id}.json"
        open_fn = open

    log.info(f"\twriting monitor data to {file_path}...")
    with open_fn(file_path, "w") as f:
        json.dump(results, f)
    log.info(f"\tdone writing monitor data to {file_path}...")
