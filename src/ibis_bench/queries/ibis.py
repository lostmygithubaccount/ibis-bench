# adapted from: https://github.com/ibis-project/ibis/tree/main/ibis/backends/tests/tpch

import ibis
import ibis.selectors as s

from datetime import date

# TODOs:
# - investigate the _right drops
# - investigate some oddities w/ Polars queries
# - ensure Ibis returns same results as Polars


def q1(lineitem, **kwargs):
    var1 = date(1998, 9, 2)

    q = (
        lineitem.filter(lineitem["l_shipdate"] <= var1)
        .group_by(["l_returnflag", "l_linestatus"])
        .aggregate(
            sum_qty=lineitem["l_quantity"].sum(),
            sum_base_price=lineitem["l_extendedprice"].sum(),
            sum_disc_price=(
                lineitem["l_extendedprice"] * (1 - lineitem["l_discount"])
            ).sum(),
            sum_charge=(
                lineitem["l_extendedprice"]
                * (1 - lineitem["l_discount"])
                * (1 + lineitem["l_tax"])
            ).sum(),
            avg_qty=lineitem["l_quantity"].mean(),
            avg_price=lineitem["l_extendedprice"].mean(),
            avg_disc=lineitem["l_discount"].mean(),
            count_order=lambda lineitem: lineitem.count(),
        )
        .order_by(["l_returnflag", "l_linestatus"])
    )
    return q


def q2(nation, part, partsupp, region, supplier, **kwargs):
    var1 = 15
    var2 = "BRASS"
    var3 = "EUROPE"

    q1 = (
        part.join(partsupp, part["p_partkey"] == partsupp["ps_partkey"])
        .drop(s.contains("_right"))
        .join(supplier, partsupp["ps_suppkey"] == supplier["s_suppkey"])
        .drop(s.contains("_right"))
        .join(nation, supplier["s_nationkey"] == nation["n_nationkey"])
        .drop(s.contains("_right"))
        .join(region, nation["n_regionkey"] == region["r_regionkey"])
        .drop(s.contains("_right"))
        .filter(ibis._["p_size"] == var1)
        .filter(ibis._["p_type"].endswith(var2))
        .filter(ibis._["r_name"] == var3)
    )

    q_final = (
        q1.group_by("p_partkey")
        .agg(ps_supplycost=ibis._["ps_supplycost"].min())
        .join(q1, ["p_partkey"])
        .select(
            "s_acctbal",
            "s_name",
            "n_name",
            "p_partkey",
            "p_mfgr",
            "s_address",
            "s_phone",
            "s_comment",
        )
        .order_by(ibis.desc("s_acctbal"), "n_name", "s_name", "p_partkey")
        .limit(100)
    )

    return q_final


def q3(customer, lineitem, orders, **kwargs):
    var1 = "BUILDING"
    var2 = date(1995, 3, 15)

    q_final = (
        customer.filter(customer["c_mktsegment"] == var1)
        .join(orders, customer["c_custkey"] == orders["o_custkey"])
        .drop(s.contains("_right"))
        .join(lineitem, orders["o_orderkey"] == lineitem["l_orderkey"])
        .drop(s.contains("_right"))
        .filter(ibis._["o_orderdate"] < var2)
        .filter(ibis._["l_shipdate"] > var2)
        .mutate(revenue=(lineitem["l_extendedprice"] * (1 - lineitem["l_discount"])))
        .group_by(
            "o_orderkey",
            "o_orderdate",
            "o_shippriority",
        )
        .agg(revenue=ibis._["revenue"].sum())
        .select(
            ibis._["o_orderkey"].name("o_orderkey"),
            "revenue",
            "o_orderdate",
            "o_shippriority",
        )
        .order_by(ibis.desc("revenue"), "o_orderdate")
        .limit(10)
    )

    return q_final


def q4(lineitem, orders, **kwargs):
    var1 = date(1993, 7, 1)
    var2 = date(1993, 10, 1)

    q_final = (
        lineitem.join(orders, lineitem["l_orderkey"] == orders["o_orderkey"])
        .filter((orders["o_orderdate"] >= var1) & (orders["o_orderdate"] < var2))
        .filter(lineitem["l_commitdate"] < lineitem["l_receiptdate"])
        .distinct(on=["o_orderpriority", "l_orderkey"])
        .group_by("o_orderpriority")
        .agg(order_count=ibis._.count())
        .order_by("o_orderpriority")
    )

    return q_final


def q5(region, nation, supplier, customer, orders, lineitem, **kwargs):
    var1 = "ASIA"
    var2 = date(1994, 1, 1)
    var3 = date(1995, 1, 1)

    q_final = (
        region.join(nation, region["r_regionkey"] == nation["n_regionkey"])
        .drop(s.contains("_right"))
        .join(customer, ibis._["n_nationkey"] == customer["c_nationkey"])
        .drop(s.contains("_right"))
        .join(orders, ibis._["c_custkey"] == orders["o_custkey"])
        .drop(s.contains("_right"))
        .join(lineitem, ibis._["o_orderkey"] == lineitem["l_orderkey"])
        .drop(s.contains("_right"))
        .join(
            supplier,
            (ibis._["l_suppkey"] == supplier["s_suppkey"])
            & (ibis._["n_nationkey"] == supplier["s_nationkey"]),
        )
        .drop(s.contains("_right"))
        .filter(ibis._["r_name"] == var1)
        .filter((ibis._["o_orderdate"] >= var2) & (ibis._["o_orderdate"] < var3))
        .mutate(revenue=(lineitem["l_extendedprice"] * (1 - lineitem["l_discount"])))
        .group_by("n_name")
        .agg(revenue=ibis._["revenue"].sum())
        .order_by(ibis.desc("revenue"))
    )

    return q_final


def q6(lineitem, **kwargs):
    var1 = date(1994, 1, 1)
    var2 = date(1995, 1, 1)
    var3 = 0.05
    var4 = 0.07
    var5 = 24

    q_final = (
        lineitem.filter(
            (lineitem["l_shipdate"] >= var1) & (lineitem["l_shipdate"] < var2)
        )
        .filter((lineitem["l_discount"] >= var3) & (lineitem["l_discount"] <= var4))
        .filter(lineitem["l_quantity"] < var5)
        .mutate(revenue=ibis._["l_extendedprice"] * (ibis._["l_discount"]))
        .agg(revenue=ibis._["revenue"].sum())
    )

    return q_final


def q7(customer, lineitem, nation, orders, supplier, **kwargs):
    var1 = "FRANCE"
    var2 = "GERMANY"
    var3 = date(1995, 1, 1)
    var4 = date(1996, 12, 31)

    n1 = nation.filter(nation["n_name"] == var1)
    n2 = nation.filter(nation["n_name"] == var2)

    q1 = (
        customer.join(n1, customer["c_nationkey"] == n1["n_nationkey"])
        .drop(s.contains("_right"))
        .join(orders, customer["c_custkey"] == orders["o_custkey"])
        .rename({"cust_nation": "n_name"})
        .drop(s.contains("_right"))
        .join(lineitem, orders["o_orderkey"] == lineitem["l_orderkey"])
        .drop(s.contains("_right"))
        .join(supplier, lineitem["l_suppkey"] == supplier["s_suppkey"])
        .drop(s.contains("_right"))
        .join(n2, supplier["s_nationkey"] == n2["n_nationkey"])
        .rename({"supp_nation": "n_name"})
    )

    q2 = (
        customer.join(n2, customer["c_nationkey"] == n2["n_nationkey"])
        .drop(s.contains("_right"))
        .join(orders, customer["c_custkey"] == orders["o_custkey"])
        .rename({"cust_nation": "n_name"})
        .drop(s.contains("_right"))
        .join(lineitem, orders["o_orderkey"] == lineitem["l_orderkey"])
        .drop(s.contains("_right"))
        .join(supplier, lineitem["l_suppkey"] == supplier["s_suppkey"])
        .drop(s.contains("_right"))
        .join(n1, supplier["s_nationkey"] == n1["n_nationkey"])
        .rename({"supp_nation": "n_name"})
    )

    q_final = (
        q1.union(q2)
        .filter((ibis._["l_shipdate"] >= var3) & (ibis._["l_shipdate"] <= var4))
        .mutate(
            volume=(ibis._["l_extendedprice"] * (1 - ibis._["l_discount"])),
            l_year=ibis._["l_shipdate"].year(),
        )
        .group_by("supp_nation", "cust_nation", "l_year")
        .agg(revenue=ibis._["volume"].sum())
        .order_by("supp_nation", "cust_nation", "l_year")
    )

    return q_final


all_queries = [q1, q2, q3, q4, q5, q6, q7]
