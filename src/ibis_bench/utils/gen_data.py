import os
import ibis

from ibis_bench.utils.logging import log


def get_data_dir(sf, n_partitions):
    return os.path.join("tpch_data", f"sf={sf}", f"n={n_partitions}")


def generate_data(sf, n_partitions):
    log.info(f"generating data for sf={sf}, n={n_partitions}...")

    con = ibis.connect("duckdb://")
    con.raw_sql("set enable_progress_bar = false")

    data_directory = get_data_dir(sf, n_partitions)

    if not os.path.exists(data_directory):
        for i in range(n_partitions):
            con.raw_sql(f"call dbgen(sf={sf}, children={n_partitions}, step={i})")
            for table in con.list_tables():
                if i == 0:
                    os.makedirs(os.path.join(data_directory, table), exist_ok=True)
                log.info(
                    f"\twriting {os.path.join(data_directory, table, f'{i:04d}.parquet')}..."
                )
                con.table(table).to_parquet(
                    os.path.join(data_directory, table, f"{i:04d}.parquet")
                )
                log.info(
                    f"\tdone writing {os.path.join(data_directory, table, f'{i:04d}.parquet')}"
                )
                con.drop_table(table)
    else:
        log.info(f"\tdata already exists at {data_directory}, skipping...")
