import ibis
import polars as pl
import ibis.selectors as s
import polars.selectors as ps

from ibis_bench.utils.gen_data import get_data_dir


def get_ibis_tables(sf, n_partitions, con=ibis.connect("duckdb://")):
    data_directory = get_data_dir(sf, n_partitions)

    customer = con.read_parquet(
        f"{data_directory}/customer/*.parquet", table_name="customer"
    )
    lineitem = con.read_parquet(
        f"{data_directory}/lineitem/*.parquet", table_name="lineitem"
    )
    nation = con.read_parquet(f"{data_directory}/nation/*.parquet", table_name="nation")
    orders = con.read_parquet(f"{data_directory}/orders/*.parquet", table_name="orders")
    part = con.read_parquet(f"{data_directory}/part/*.parquet", table_name="part")
    partsupp = con.read_parquet(
        f"{data_directory}/partsupp/*.parquet", table_name="partsupp"
    )
    region = con.read_parquet(f"{data_directory}/region/*.parquet", table_name="region")
    supplier = con.read_parquet(
        f"{data_directory}/supplier/*.parquet", table_name="supplier"
    )

    # TODO: report issue and remove; also duplicate for Polars native
    def _decimal_to_float(t):
        return t.mutate(
            s.across(
                s.of_type("decimal"),
                ibis._.cast("float"),
            )
        )

    customer = _decimal_to_float(customer)
    lineitem = _decimal_to_float(lineitem)
    nation = _decimal_to_float(nation)
    orders = _decimal_to_float(orders)
    part = _decimal_to_float(part)
    partsupp = _decimal_to_float(partsupp)
    region = _decimal_to_float(region)
    supplier = _decimal_to_float(supplier)

    return customer, lineitem, nation, orders, part, partsupp, region, supplier


def get_polars_tables(sf, n_partitions, lazy=True):
    import os

    os.environ["POLARS_ACTIVATE_DECIMAL"] = (
        "1"  # https://github.com/pola-rs/polars/issues/16603#issuecomment-2141701041
    )
    data_directory = get_data_dir(sf, n_partitions)

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

    # TODO: report issue and remove
    def _decimal_to_float(df):
        return df.with_columns((ps.decimal().cast(pl.Float32)))

    customer = _decimal_to_float(customer)
    lineitem = _decimal_to_float(lineitem)
    nation = _decimal_to_float(nation)
    orders = _decimal_to_float(orders)
    part = _decimal_to_float(part)
    partsupp = _decimal_to_float(partsupp)
    region = _decimal_to_float(region)
    supplier = _decimal_to_float(supplier)

    return customer, lineitem, nation, orders, part, partsupp, region, supplier
