import ibis
import polars as pl
import ibis.selectors as s
import polars.selectors as ps

from ibis_bench.tpcds.utils.gen_data import get_data_dir


def get_ibis_tables(
    sf, con=ibis.connect("duckdb://"), csv=False, decimal_to_float=False
):
    data_directory = get_data_dir(sf, csv=csv)

    if not csv:
        call_center = con.read_parquet(
            f"{data_directory}/call_center/*.parquet", table_name="call_center"
        )
        catalog_page = con.read_parquet(
            f"{data_directory}/catalog_page/*.parquet", table_name="catalog_page"
        )
        catalog_returns = con.read_parquet(
            f"{data_directory}/catalog_returns/*.parquet", table_name="catalog_returns"
        )
        catalog_sales = con.read_parquet(
            f"{data_directory}/catalog_sales/*.parquet", table_name="catalog_sales"
        )
        customer = con.read_parquet(
            f"{data_directory}/customer/*.parquet", table_name="customer"
        )
        customer_address = con.read_parquet(
            f"{data_directory}/customer_address/*.parquet",
            table_name="customer_address",
        )
        customer_demographics = con.read_parquet(
            f"{data_directory}/customer_demographics/*.parquet",
            table_name="customer_demographics",
        )
        date_dim = con.read_parquet(
            f"{data_directory}/date_dim/*.parquet", table_name="date_dim"
        )
        household_demographics = con.read_parquet(
            f"{data_directory}/household_demographics/*.parquet",
            table_name="household_demographics",
        )
        income_band = con.read_parquet(
            f"{data_directory}/income_band/*.parquet", table_name="income_band"
        )
        inventory = con.read_parquet(
            f"{data_directory}/inventory/*.parquet", table_name="inventory"
        )
        item = con.read_parquet(f"{data_directory}/item/*.parquet", table_name="item")
        promotion = con.read_parquet(
            f"{data_directory}/promotion/*.parquet", table_name="promotion"
        )
        reason = con.read_parquet(
            f"{data_directory}/reason/*.parquet", table_name="reason"
        )
        ship_mode = con.read_parquet(
            f"{data_directory}/ship_mode/*.parquet", table_name="ship_mode"
        )
        store = con.read_parquet(
            f"{data_directory}/store/*.parquet", table_name="store"
        )
        store_returns = con.read_parquet(
            f"{data_directory}/store_returns/*.parquet", table_name="store_returns"
        )
        store_sales = con.read_parquet(
            f"{data_directory}/store_sales/*.parquet", table_name="store_sales"
        )
        time_dim = con.read_parquet(
            f"{data_directory}/time_dim/*.parquet", table_name="time_dim"
        )
        warehouse = con.read_parquet(
            f"{data_directory}/warehouse/*.parquet", table_name="warehouse"
        )
        web_page = con.read_parquet(
            f"{data_directory}/web_page/*.parquet", table_name="web_page"
        )
        web_returns = con.read_parquet(
            f"{data_directory}/web_returns/*.parquet", table_name="web_returns"
        )
        web_sales = con.read_parquet(
            f"{data_directory}/web_sales/*.parquet", table_name="web_sales"
        )
        web_site = con.read_parquet(
            f"{data_directory}/web_site/*.parquet", table_name="web_site"
        )
    else:
        call_center == con.read_csv(
            f"{data_directory}/call_center/*.csv", table_name="call_center"
        )
        catalog_page = con.read_csv(
            f"{data_directory}/catalog_page/*.csv", table_name="catalog_page"
        )
        catalog_returns = con.read_csv(
            f"{data_directory}/catalog_returns/*.csv", table_name="catalog_returns"
        )
        catalog_sales = con.read_csv(
            f"{data_directory}/catalog_sales/*.csv", table_name="catalog_sales"
        )
        customer = con.read_csv(
            f"{data_directory}/customer/*.csv", table_name="customer"
        )
        customer_address = con.read_csv(
            f"{data_directory}/customer_address/*.csv", table_name="customer_address"
        )
        customer_demographics = con.read_csv(
            f"{data_directory}/customer_demographics/*.csv",
            table_name="customer_demographics",
        )
        date_dim = con.read_csv(
            f"{data_directory}/date_dim/*.csv", table_name="date_dim"
        )
        household_demographics = con.read_csv(
            f"{data_directory}/household_demographics/*.csv",
            table_name="household_demographics",
        )
        income_band = con.read_csv(
            f"{data_directory}/income_band/*.csv", table_name="income_band"
        )
        inventory = con.read_csv(
            f"{data_directory}/inventory/*.csv", table_name="inventory"
        )
        item = con.read_csv(f"{data_directory}/item/*.csv", table_name="item")
        promotion = con.read_csv(
            f"{data_directory}/promotion/*.csv", table_name="promotion"
        )
        reason = con.read_csv(f"{data_directory}/reason/*.csv", table_name="reason")
        ship_mode = con.read_csv(
            f"{data_directory}/ship_mode/*.csv", table_name="ship_mode"
        )
        store = con.read_csv(f"{data_directory}/store/*.csv", table_name="store")
        store_returns = con.read_csv(
            f"{data_directory}/store_returns/*.csv", table_name="store_returns"
        )
        store_sales = con.read_csv(
            f"{data_directory}/store_sales/*.csv", table_name="store_sales"
        )
        time_dim = con.read_csv(
            f"{data_directory}/time_dim/*.csv", table_name="time_dim"
        )
        warehouse = con.read_csv(
            f"{data_directory}/warehouse/*.csv", table_name="warehouse"
        )
        web_page = con.read_csv(
            f"{data_directory}/web_page/*.csv", table_name="web_page"
        )
        web_returns = con.read_csv(
            f"{data_directory}/web_returns/*.csv", table_name="web_returns"
        )
        web_sales = con.read_csv(
            f"{data_directory}/web_sales/*.csv", table_name="web_sales"
        )
        web_site = con.read_csv(
            f"{data_directory}/web_site/*.csv", table_name="web_site"
        )

    all_tables = (
        call_center,
        catalog_page,
        catalog_returns,
        catalog_sales,
        customer,
        customer_address,
        customer_demographics,
        date_dim,
        household_demographics,
        income_band,
        inventory,
        item,
        promotion,
        reason,
        ship_mode,
        store,
        store_returns,
        store_sales,
        time_dim,
        warehouse,
        web_page,
        web_returns,
        web_sales,
        web_site,
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
        all_tables = tuple(map(_decimal_to_float, all_tables))

    # TODO: keep this or figure something out and remove
    def _drop_hive_cols(t):
        # NOTE: some backends don't create the hive-partitioned columns (at least by default)
        # DuckDB and Polars do, DataFusion doesn't, so first check if the column(s) exist
        if "sf" in t.columns:
            t = t.drop("sf")
        if "n" in t.columns:
            t = t.drop("n")
        return t

    all_tables = tuple(map(_drop_hive_cols, all_tables))

    return all_tables
