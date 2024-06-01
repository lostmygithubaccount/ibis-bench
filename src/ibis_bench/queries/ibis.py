import ibis
import ibis.selectors as s

from datetime import date


def q1(lineitem, **kwargs):
    t = lineitem

    var1 = date(1998, 9, 2)

    q = (
        t.filter(t.l_shipdate <= var1)
        .group_by(["l_returnflag", "l_linestatus"])
        .aggregate(
            sum_qty=t.l_quantity.sum(),
            sum_base_price=t.l_extendedprice.sum(),
            sum_disc_price=(t.l_extendedprice * (1 - t.l_discount)).sum(),
            sum_charge=(t.l_extendedprice * (1 - t.l_discount) * (1 + t.l_tax)).sum(),
            avg_qty=t.l_quantity.mean(),
            avg_price=t.l_extendedprice.mean(),
            avg_disc=t.l_discount.mean(),
            count_order=lambda t: t.count(),
        )
        .order_by(["l_returnflag", "l_linestatus"])
    )
    return q


def q2(nation, part, partsupp, region, supplier, **kwargs):
    var1 = 15
    var2 = "BRASS"
    var3 = "EUROPE"

    # TODO: investigate the _right drops
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


all_queries = [q1, q2]
