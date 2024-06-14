import os
import ibis

from ibis_bench.utils.logging import log


def get_data_dir(sf, n_partitions, csv: bool = False):
    dir_name = "csv" if csv else "parquet"
    return os.path.join("tpch_data", dir_name, f"sf={sf}", f"n={n_partitions}")


def generate_data(sf, n_partitions, csv: bool = False):
    log.info(f"generating data for sf={sf}, n={n_partitions}...")

    con = ibis.connect("duckdb://")
    con.raw_sql("PRAGMA disable_progress_bar;")

    parquet_data_directory = get_data_dir(sf, n_partitions)
    csv_data_directory = get_data_dir(sf, n_partitions, csv=True)

    if not os.path.exists(parquet_data_directory):
        for i in range(n_partitions):
            con.raw_sql(f"call dbgen(sf={sf}, children={n_partitions}, step={i})")
            for table in con.list_tables():
                if i == 0:
                    os.makedirs(
                        os.path.join(parquet_data_directory, table), exist_ok=True
                    )
                    if csv:
                        os.makedirs(
                            os.path.join(csv_data_directory, table), exist_ok=True
                        )
                log.info(
                    f"\twriting {os.path.join(parquet_data_directory, table, f'{i:04d}.parquet')}..."
                )
                con.table(table).to_parquet(
                    os.path.join(parquet_data_directory, table, f"{i:04d}.parquet")
                )
                log.info(
                    f"\tdone writing {os.path.join(parquet_data_directory, table, f'{i:04d}.parquet')}"
                )
                if csv:
                    log.info(
                        f"\twriting {os.path.join(csv_data_directory, table, f'{i:04d}.csv')}..."
                    )
                    con.table(table).to_csv(
                        os.path.join(csv_data_directory, table, f"{i:04d}.csv")
                    )
                    log.info(
                        f"\tdone writing {os.path.join(csv_data_directory, table, f'{i:04d}.csv')}"
                    )

                con.drop_table(table)
    else:
        log.info(f"\tdata already exists at {parquet_data_directory}, skipping...")


def _parquets_to_csvs():
    con = ibis.connect("duckdb://")
    for root, _, files in os.walk(os.path.join("tpch_data", "parquet")):
        for file in files:
            if file.endswith(".parquet"):
                parquet_file = os.path.join(root, file)
                csv_file = parquet_file.replace("parquet", "csv")

                os.makedirs(os.path.dirname(csv_file), exist_ok=True)

                log.info(f"reading {parquet_file}...")
                t = con.read_parquet(parquet_file)

                log.info(f"writing {csv_file}...")
                t.to_csv(csv_file)
