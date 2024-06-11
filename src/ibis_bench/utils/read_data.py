import ibis
import polars as pl
import ibis.selectors as s
import polars.selectors as ps

from ibis_bench.utils.gen_data import get_data_dir


def get_ibis_tables(
    sf, n_partitions=1, con=ibis.connect("duckdb://"), csv=False, decimal_to_float=True
):
    data_directory = get_data_dir(sf, n_partitions, csv=csv)

    if not csv:
        customer = con.read_parquet(
            f"{data_directory}/customer/*.parquet", table_name="customer"
        )
        lineitem = con.read_parquet(
            f"{data_directory}/lineitem/*.parquet", table_name="lineitem"
        )
        nation = con.read_parquet(
            f"{data_directory}/nation/*.parquet", table_name="nation"
        )
        orders = con.read_parquet(
            f"{data_directory}/orders/*.parquet", table_name="orders"
        )
        part = con.read_parquet(f"{data_directory}/part/*.parquet", table_name="part")
        partsupp = con.read_parquet(
            f"{data_directory}/partsupp/*.parquet", table_name="partsupp"
        )
        region = con.read_parquet(
            f"{data_directory}/region/*.parquet", table_name="region"
        )
        supplier = con.read_parquet(
            f"{data_directory}/supplier/*.parquet", table_name="supplier"
        )
    else:
        customer = con.read_csv(
            f"{data_directory}/customer/*.csv", table_name="customer"
        )
        lineitem = con.read_csv(
            f"{data_directory}/lineitem/*.csv", table_name="lineitem"
        )
        nation = con.read_csv(f"{data_directory}/nation/*.csv", table_name="nation")
        orders = con.read_csv(f"{data_directory}/orders/*.csv", table_name="orders")
        part = con.read_csv(f"{data_directory}/part/*.csv", table_name="part")
        partsupp = con.read_csv(
            f"{data_directory}/partsupp/*.csv", table_name="partsupp"
        )
        region = con.read_csv(f"{data_directory}/region/*.csv", table_name="region")
        supplier = con.read_csv(
            f"{data_directory}/supplier/*.csv", table_name="supplier"
        )

    # TODO: report issue(s) (DataFusion backend issue)
    def _decimal_to_float(t):
        return t.mutate(
            s.across(
                s.of_type("decimal"),
                ibis._.cast("float"),
            )
        )

    if decimal_to_float:
        customer = _decimal_to_float(customer)
        lineitem = _decimal_to_float(lineitem)
        nation = _decimal_to_float(nation)
        orders = _decimal_to_float(orders)
        part = _decimal_to_float(part)
        partsupp = _decimal_to_float(partsupp)
        region = _decimal_to_float(region)
        supplier = _decimal_to_float(supplier)

    # TODO: keep this or figure something out and remove
    def _drop_hive_cols(t):
        # NOTE: some backends don't create the hive-partitioned columns (at least by default)
        # DuckDB and Polars do, DataFusion doesn't, so first check if the column(s) exist
        if "sf" in t.columns:
            t = t.drop("sf")
        if "n" in t.columns:
            t = t.drop("n")
        return t

    customer = _drop_hive_cols(customer)
    lineitem = _drop_hive_cols(lineitem)
    nation = _drop_hive_cols(nation)
    orders = _drop_hive_cols(orders)
    part = _drop_hive_cols(part)
    partsupp = _drop_hive_cols(partsupp)
    region = _drop_hive_cols(region)
    supplier = _drop_hive_cols(supplier)

    return customer, lineitem, nation, orders, part, partsupp, region, supplier


def get_polars_tables(sf, n_partitions=1, lazy=True, csv=False, decimal_to_float=True):
    import os

    # TODO: remove after Polars v1.0.0
    os.environ["POLARS_ACTIVATE_DECIMAL"] = (
        "1"  # https://github.com/pola-rs/polars/issues/16603#issuecomment-2141701041
    )
    data_directory = get_data_dir(sf, n_partitions, csv=csv)

    if not csv:
        if lazy:
            customer = pl.scan_parquet(f"{data_directory}/customer/*.parquet")
            lineitem = pl.scan_parquet(f"{data_directory}/lineitem/*.parquet")
            nation = pl.scan_parquet(f"{data_directory}/nation/*.parquet")
            orders = pl.scan_parquet(f"{data_directory}/orders/*.parquet")
            part = pl.scan_parquet(f"{data_directory}/part/*.parquet")
            partsupp = pl.scan_parquet(f"{data_directory}/partsupp/*.parquet")
            region = pl.scan_parquet(f"{data_directory}/region/*.parquet")
            supplier = pl.scan_parquet(f"{data_directory}/supplier/*.parquet")
        else:
            customer = pl.read_parquet(f"{data_directory}/customer/*.parquet")
            lineitem = pl.read_parquet(f"{data_directory}/lineitem/*.parquet")
            nation = pl.read_parquet(f"{data_directory}/nation/*.parquet")
            orders = pl.read_parquet(f"{data_directory}/orders/*.parquet")
            part = pl.read_parquet(f"{data_directory}/part/*.parquet")
            partsupp = pl.read_parquet(f"{data_directory}/partsupp/*.parquet")
            region = pl.read_parquet(f"{data_directory}/region/*.parquet")
            supplier = pl.read_parquet(f"{data_directory}/supplier/*.parquet")
    else:
        if lazy:
            customer = pl.scan_csv(f"{data_directory}/customer/*.csv")
            lineitem = pl.scan_csv(f"{data_directory}/lineitem/*.csv")
            nation = pl.scan_csv(f"{data_directory}/nation/*.csv")
            orders = pl.scan_csv(f"{data_directory}/orders/*.csv")
            part = pl.scan_csv(f"{data_directory}/part/*.csv")
            partsupp = pl.scan_csv(f"{data_directory}/partsupp/*.csv")
            region = pl.scan_csv(f"{data_directory}/region/*.csv")
            supplier = pl.scan_csv(f"{data_directory}/supplier/*.csv")
        else:
            customer = pl.read_csv(f"{data_directory}/customer/*.csv")
            lineitem = pl.read_csv(f"{data_directory}/lineitem/*.csv")
            nation = pl.read_csv(f"{data_directory}/nation/*.csv")
            orders = pl.read_csv(f"{data_directory}/orders/*.csv")
            part = pl.read_csv(f"{data_directory}/part/*.csv")
            partsupp = pl.read_csv(f"{data_directory}/partsupp/*.csv")
            region = pl.read_csv(f"{data_directory}/region/*.csv")
            supplier = pl.read_csv(f"{data_directory}/supplier/*.csv")

    # TODO: report issue(s) (issue(s) at higher SFs)
    def _decimal_to_float(df):
        return df.with_columns((ps.decimal().cast(pl.Float64)))

    if decimal_to_float:
        customer = _decimal_to_float(customer)
        lineitem = _decimal_to_float(lineitem)
        nation = _decimal_to_float(nation)
        orders = _decimal_to_float(orders)
        part = _decimal_to_float(part)
        partsupp = _decimal_to_float(partsupp)
        region = _decimal_to_float(region)
        supplier = _decimal_to_float(supplier)

    # TODO: keep this or figure something out and remove
    def _drop_hive_cols(df):
        return df.drop(["sf", "n"])

    customer = _drop_hive_cols(customer)
    lineitem = _drop_hive_cols(lineitem)
    nation = _drop_hive_cols(nation)
    orders = _drop_hive_cols(orders)
    part = _drop_hive_cols(part)
    partsupp = _drop_hive_cols(partsupp)
    region = _drop_hive_cols(region)
    supplier = _drop_hive_cols(supplier)

    return customer, lineitem, nation, orders, part, partsupp, region, supplier
