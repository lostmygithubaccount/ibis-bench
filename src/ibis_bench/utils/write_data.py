import os
import ibis
import polars as pl

from ibis_bench.utils.logging import log


def write_results(
    res,
    sf: int,
    n_partitions: int,
    system: str,
    q_number: int,
):
    dirname = os.path.join("results_data", system)

    if not os.path.exists(dirname):
        os.makedirs(dirname, exist_ok=True)

    if isinstance(res, ibis.Table):
        log.info(
            f"\twriting ibis result to {os.path.join(dirname, f'q_{q_number}.parquet')}..."
        )
        res.to_parquet(os.path.join(dirname, f"q_{q_number}.parquet"))
    elif isinstance(res, pl.DataFrame):
        log.info(
            f"\twriting polars (eager) result to {os.path.join(dirname, f'q_{q_number}.parquet')}..."
        )
        res.write_parquet(os.path.join(dirname, f"q_{q_number}.parquet"))
    elif isinstance(res, pl.LazyFrame):
        log.info(
            f"\twriting polars (lazy) result to {os.path.join(dirname, f'q_{q_number}.parquet')}..."
        )
        try:
            # https://github.com/pola-rs/polars/issues/6603
            # InvalidOperationError: sink_Parquet(ParquetWriteOptions { compression: Zstd(None), statistics: true, row_group_size: None, data_pagesize_limit: None, maintain_order: true }) not yet supported in standard engine. Use 'collect().write_parquet()'
            # "not all queries are supported for sinking"
            res.sink_parquet(os.path.join(dirname, f"q_{q_number}.parquet"))
        except:
            res.collect().write_parquet(os.path.join(dirname, f"q_{q_number}.parquet"))

    del res
