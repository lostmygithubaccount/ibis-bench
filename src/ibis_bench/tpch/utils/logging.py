import os
import sys
import logging as log

from datetime import datetime

# TODO: this is all very hacky
file_logging = False

if file_logging:
    log_name = str(datetime.now().strftime("%Y-%m-%d:%s"))
    dir_name = "bench_cli_logs"

    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    log.basicConfig(
        level=log.INFO,
        filename=os.path.join(dir_name, f"ibis_bench_{log_name}.log"),
        filemode="a",
        format=" %(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # sys.stdout = open(
    #    os.path.join(dir_name, f"ibis_bench_{log_name}_stdout.log"),
    #    "a",
    # )
    sys.stderr = open(
        os.path.join(dir_name, f"ibis_bench_{log_name}_stderr.log"),
        "a",
    )

else:
    log.basicConfig(level=log.INFO)
