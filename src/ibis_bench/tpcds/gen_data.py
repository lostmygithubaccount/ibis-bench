import os
import ibis

from ibis_bench.utils.logging import log


def get_data_dir(sf, csv: bool = False):
    dir_name = "csv" if csv else "parquet"
    return os.path.join("tpcds_data", dir_name, f"sf={sf}")


def generate_data(sf, csv: bool = False):
    log.info(f"generating data for sf={sf}...")

    con = ibis.connect("duckdb://")
    con.raw_sql("PRAGMA disable_progress_bar;")

    parquet_data_directory = get_data_dir(sf)
    csv_data_directory = get_data_dir(sf, csv=True)

    if not os.path.exists(parquet_data_directory):
        con.raw_sql(f"call dsdgen(sf={sf})")
        for table in con.list_tables():
            os.makedirs(os.path.join(parquet_data_directory, table), exist_ok=True)
            if csv:
                os.makedirs(os.path.join(csv_data_directory, table), exist_ok=True)
            log.info(
                f"\twriting {os.path.join(parquet_data_directory, table, f'{0:04d}.parquet')}..."
            )
            con.table(table).to_parquet(
                os.path.join(parquet_data_directory, table, f"{0:04d}.parquet")
            )
            log.info(
                f"\tdone writing {os.path.join(parquet_data_directory, table, f'{0:04d}.parquet')}"
            )
            if csv:
                log.info(
                    f"\twriting {os.path.join(csv_data_directory, table, f'{0:04d}.csv')}..."
                )
                con.table(table).to_csv(
                    os.path.join(csv_data_directory, table, f"{0:04d}.csv")
                )
                log.info(
                    f"\tdone writing {os.path.join(csv_data_directory, table, f'{0:04d}.csv')}"
                )

            con.drop_table(table)
    else:
        log.info(f"\tdata already exists at {parquet_data_directory}, skipping...")


def _parquets_to_csvs():
    con = ibis.connect("duckdb://")
    for root, _, files in os.walk(os.path.join("tpcds_data", "parquet")):
        for file in files:
            if file.endswith(".parquet"):
                parquet_file = os.path.join(root, file)
                csv_file = parquet_file.replace("parquet", "csv")

                os.makedirs(os.path.dirname(csv_file), exist_ok=True)

                log.info(f"reading {parquet_file}...")
                t = con.read_parquet(parquet_file)

                log.info(f"writing {csv_file}...")
                t.to_csv(csv_file)
