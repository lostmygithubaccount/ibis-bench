import logging as log

file_logging = False

if file_logging:
    log.basicConfig(
        level=log.INFO,
        filename="ibis_bench.log",
        filemode="a",
        format=" %(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
else:
    log.basicConfig(level=log.INFO)
