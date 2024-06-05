import os
import logging as log

from datetime import datetime

file_logging = True

dir_name = "bench_cli_logs"

if not os.path.exists(dir_name):
    os.makedirs(dir_name, exist_ok=True)

if file_logging:
    log.basicConfig(
        level=log.INFO,
        filename=os.path.join(
            dir_name, f"ibis_bench_{datetime.now().strftime('%Y-%m-%d:%s')}.log"
        ),
        filemode="a",
        format=" %(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
else:
    log.basicConfig(level=log.INFO)
