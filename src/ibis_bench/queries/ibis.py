# adapted from corresponding Polars queries (first ten)
# also adapted from: https://github.com/ibis-project/ibis/tree/main/ibis/backends/tests/tpch

import ibis

from datetime import date


def q1(lineitem, **kwargs):
    var1 = date(1998, 9, 2)

    q_final = (
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
    return q_final


def q2(nation, part, partsupp, region, supplier, **kwargs):
    var1 = 15
    var2 = "BRASS"
    var3 = "EUROPE"

    q1 = (
        part.join(partsupp, part["p_partkey"] == partsupp["ps_partkey"])
        .join(supplier, partsupp["ps_suppkey"] == supplier["s_suppkey"])
        .join(nation, supplier["s_nationkey"] == nation["n_nationkey"])
        .join(region, nation["n_regionkey"] == region["r_regionkey"])
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
        .join(lineitem, orders["o_orderkey"] == lineitem["l_orderkey"])
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
        .join(customer, ibis._["n_nationkey"] == customer["c_nationkey"])
        .join(orders, ibis._["c_custkey"] == orders["o_custkey"])
        .join(lineitem, ibis._["o_orderkey"] == lineitem["l_orderkey"])
        .join(
            supplier,
            (ibis._["l_suppkey"] == supplier["s_suppkey"])
            & (ibis._["n_nationkey"] == supplier["s_nationkey"]),
        )
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
        .join(orders, customer["c_custkey"] == orders["o_custkey"])
        .rename({"cust_nation": "n_name"})
        .join(lineitem, orders["o_orderkey"] == lineitem["l_orderkey"])
        .join(supplier, lineitem["l_suppkey"] == supplier["s_suppkey"])
        .join(n2, supplier["s_nationkey"] == n2["n_nationkey"])
        .rename({"supp_nation": "n_name"})
    )

    q2 = (
        customer.join(n2, customer["c_nationkey"] == n2["n_nationkey"])
        .join(orders, customer["c_custkey"] == orders["o_custkey"])
        .rename({"cust_nation": "n_name"})
        .join(lineitem, orders["o_orderkey"] == lineitem["l_orderkey"])
        .join(supplier, lineitem["l_suppkey"] == supplier["s_suppkey"])
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


def q8(customer, lineitem, nation, orders, part, region, supplier, **kwargs):
    var1 = "BRAZIL"
    var2 = "AMERICA"
    var3 = "ECONOMY ANODIZED STEEL"
    var4 = date(1995, 1, 1)
    var5 = date(1996, 12, 31)

    n1 = nation.select("n_nationkey", "n_regionkey")
    n2 = nation.select("n_nationkey", "n_name")

    q_final = (
        part.join(lineitem, lineitem["l_partkey"] == part["p_partkey"])
        .join(supplier, lineitem["l_suppkey"] == supplier["s_suppkey"])
        .join(orders, lineitem["l_orderkey"] == orders["o_orderkey"])
        .join(customer, orders["o_custkey"] == customer["c_custkey"])
        .join(n1, customer["c_nationkey"] == n1["n_nationkey"])
        .join(region, n1["n_regionkey"] == region["r_regionkey"])
        .filter(region["r_name"] == var2)
        .join(n2, supplier["s_nationkey"] == n2["n_nationkey"])
        .filter((orders["o_orderdate"] >= var4) & (orders["o_orderdate"] <= var5))
        .filter(part["p_type"] == var3)
        .select(
            o_year=orders["o_orderdate"].year(),
            volume=lineitem["l_extendedprice"] * (1 - lineitem["l_discount"]),
            nation=n2["n_name"],
        )
        .mutate(
            _tmp=ibis.case()
            .when(
                ibis._["nation"] == var1,
                ibis._["volume"],
            )
            .else_(0)
            .end()
        )
        .group_by("o_year")
        .agg(mkt_share=(ibis._["_tmp"].sum() / ibis._["volume"].sum()).round(2))
        .order_by("o_year")
    )

    return q_final


def q9(lineitem, nation, orders, part, partsupp, supplier, **kwargs):
    q_final = (
        lineitem.join(supplier, lineitem["l_suppkey"] == supplier["s_suppkey"])
        .join(
            partsupp,
            (lineitem["l_suppkey"] == partsupp["ps_suppkey"])
            & (lineitem["l_partkey"] == partsupp["ps_partkey"]),
        )
        .join(part, lineitem["l_partkey"] == part["p_partkey"])
        .join(orders, lineitem["l_orderkey"] == orders["o_orderkey"])
        .join(nation, supplier["s_nationkey"] == nation["n_nationkey"])
        .filter(ibis._["p_name"].contains("green"))
        .select(
            nation=ibis._["n_name"],
            o_year=ibis._["o_orderdate"].year(),
            amount=ibis._["l_extendedprice"] * (1 - ibis._["l_discount"])
            - ibis._["ps_supplycost"] * ibis._["l_quantity"],
        )
        .group_by("nation", "o_year")
        .agg(sum_profit=ibis._["amount"].sum().round(2))
        .order_by("nation", ibis.desc("o_year"))
    )

    return q_final


def q10(customer, lineitem, nation, orders, **kwargs):
    var1 = date(1993, 10, 1)
    var2 = date(1994, 1, 1)

    q_final = (
        customer.join(orders, customer["c_custkey"] == orders["o_custkey"])
        .join(lineitem, orders["o_orderkey"] == lineitem["l_orderkey"])
        .join(nation, customer["c_nationkey"] == nation["n_nationkey"])
        .filter((ibis._["o_orderdate"] >= var1) & (ibis._["o_orderdate"] < var2))
        .filter(ibis._["l_returnflag"] == "R")
        .group_by(
            "c_custkey",
            "c_name",
            "c_acctbal",
            "c_phone",
            "n_name",
            "c_address",
            "c_comment",
        )
        .agg(
            revenue=(
                (ibis._["l_extendedprice"] * (1 - ibis._["l_discount"])).sum().round(2)
            )
        )
        .select(
            "c_custkey",
            "c_name",
            "revenue",
            "c_acctbal",
            "n_name",
            "c_address",
            "c_phone",
            "c_comment",
        )
        .order_by(ibis.desc("revenue"))
        .limit(20)
    )

    return q_final


def q11(nation, partsupp, supplier, **kwargs):
    NATION = "GERMANY"
    FRACTION = 0.0001

    q = partsupp
    q = q.join(supplier, partsupp.ps_suppkey == supplier.s_suppkey)
    q = q.join(nation, nation.n_nationkey == supplier.s_nationkey)

    q = q.filter([q.n_name == NATION])

    innerq = partsupp
    innerq = innerq.join(supplier, partsupp.ps_suppkey == supplier.s_suppkey)
    innerq = innerq.join(nation, nation.n_nationkey == supplier.s_nationkey)
    innerq = innerq.filter([innerq.n_name == NATION])
    innerq = innerq.aggregate(total=(innerq.ps_supplycost * innerq.ps_availqty).sum())

    gq = q.group_by([q.ps_partkey])
    q = gq.aggregate(value=(q.ps_supplycost * q.ps_availqty).sum())
    q = q.filter([q.value > innerq.total * FRACTION])
    q = q.order_by(ibis.desc(q.value))

    return q


def q12(orders, lineitem, **kwargs):
    var1 = "MAIL"
    var2 = "SHIP"
    var3 = date(1994, 1, 1)
    var4 = date(1995, 1, 1)

    q = orders
    q = q.join(lineitem, orders.o_orderkey == lineitem.l_orderkey)

    q = q.filter(
        [
            q.l_shipmode.isin([var1, var2]),
            q.l_commitdate < q.l_receiptdate,
            q.l_shipdate < q.l_commitdate,
            q.l_receiptdate >= ibis.date(var3),
            q.l_receiptdate < var4,
        ]
    )

    gq = q.group_by([q.l_shipmode])
    q = gq.aggregate(
        high_line_count=(
            q.o_orderpriority.case()
            .when("1-URGENT", 1)
            .when("2-HIGH", 1)
            .else_(0)
            .end()
        ).sum(),
        low_line_count=(
            q.o_orderpriority.case()
            .when("1-URGENT", 0)
            .when("2-HIGH", 0)
            .else_(1)
            .end()
        ).sum(),
    )
    q = q.order_by(q.l_shipmode)

    return q


def q13(customer, orders, **kwargs):
    var1 = "special"
    var2 = "requests"

    innerq = customer
    innerq = innerq.left_join(
        orders,
        (customer.c_custkey == orders.o_custkey)
        & ~orders.o_comment.like(f"%{var1}%{var2}%"),
    )
    innergq = innerq.group_by([innerq.c_custkey])
    innerq = innergq.aggregate(c_count=innerq.o_orderkey.count())

    gq = innerq.group_by([innerq.c_count])
    q = gq.aggregate(custdist=innerq.count())

    q = q.order_by([ibis.desc(q.custdist), ibis.desc(q.c_count)])
    return q


def q14(part, lineitem, **kwargs):
    var1 = date(1995, 9, 1)
    var2 = date(1995, 10, 1)

    q = lineitem
    q = q.join(part, lineitem.l_partkey == part.p_partkey)
    q = q.filter([q.l_shipdate >= var1, q.l_shipdate < var2])

    revenue = q.l_extendedprice * (1 - q.l_discount)
    promo_revenue = q.p_type.like("PROMO%").ifelse(revenue, 0)

    q = q.aggregate(promo_revenue=100 * promo_revenue.sum() / revenue.sum())
    return q


def q15(lineitem, supplier, **kwargs):
    var1 = date(1996, 1, 1)
    var2 = date(1996, 4, 1)

    qrev = lineitem
    qrev = qrev.filter(
        [
            lineitem.l_shipdate >= ibis.date(var1),
            lineitem.l_shipdate < var2,
        ]
    )

    gqrev = qrev.group_by([lineitem.l_suppkey])
    qrev = gqrev.aggregate(
        total_revenue=(qrev.l_extendedprice * (1 - qrev.l_discount)).sum()
    )

    q = supplier.join(qrev, supplier.s_suppkey == qrev.l_suppkey)
    q = q.filter([q.total_revenue == qrev.total_revenue.max()])
    q = q[q.s_suppkey, q.s_name, q.s_address, q.s_phone, q.total_revenue]
    return q.order_by([q.s_suppkey])


def q16(partsupp, part, supplier, **kwargs):
    BRAND = "Brand#45"
    TYPE = "MEDIUM POLISHED"
    SIZES = (49, 14, 23, 45, 19, 3, 36, 9)

    q = partsupp.join(part, part.p_partkey == partsupp.ps_partkey)
    q = q.filter(
        [
            q.p_brand != BRAND,
            ~q.p_type.like(f"{TYPE}%"),
            q.p_size.isin(SIZES),
            ~q.ps_suppkey.isin(
                supplier.filter(
                    [supplier.s_comment.like("%Customer%Complaints%")]
                ).s_suppkey
            ),
        ]
    )
    gq = q.group_by([q.p_brand, q.p_type, q.p_size])
    q = gq.aggregate(supplier_cnt=q.ps_suppkey.nunique())
    q = q.order_by([ibis.desc(q.supplier_cnt), q.p_brand, q.p_type, q.p_size])
    return q


def q17(lineitem, part, **kwargs):
    BRAND = "Brand#23"
    CONTAINER = "MED BOX"

    q = lineitem.join(part, part.p_partkey == lineitem.l_partkey)

    innerq = lineitem
    innerq = innerq.filter([innerq.l_partkey == q.p_partkey])

    q = q.filter(
        [
            q.p_brand == BRAND,
            q.p_container == CONTAINER,
            q.l_quantity < (0.2 * innerq.l_quantity.mean()),
        ]
    )
    q = q.aggregate(avg_yearly=q.l_extendedprice.sum() / 7.0)
    return q


def q18(customer, orders, lineitem, **kwargs):
    QUANTITY = 300

    subgq = lineitem.group_by([lineitem.l_orderkey])
    subq = subgq.aggregate(qty_sum=lineitem.l_quantity.sum())
    subq = subq.filter([subq.qty_sum > QUANTITY])

    q = customer
    q = q.join(orders, customer.c_custkey == orders.o_custkey)
    q = q.join(lineitem, orders.o_orderkey == lineitem.l_orderkey)
    q = q.filter([q.o_orderkey.isin(subq.l_orderkey)])

    gq = q.group_by(
        [q.c_name, q.c_custkey, q.o_orderkey, q.o_orderdate, q.o_totalprice]
    )
    q = gq.aggregate(sum_qty=q.l_quantity.sum())
    q = q.order_by([ibis.desc(q.o_totalprice), q.o_orderdate])
    return q.limit(100)


def q19(lineitem, part, **kwargs):
    QUANTITY1 = 1
    QUANTITY2 = 10
    QUANTITY3 = 20
    BRAND1 = "Brand#12"
    BRAND2 = "Brand#23"
    BRAND3 = "Brand#34"

    q = lineitem.join(part, part.p_partkey == lineitem.l_partkey)

    q1 = (
        (q.p_brand == BRAND1)
        & (q.p_container.isin(("SM CASE", "SM BOX", "SM PACK", "SM PKG")))
        & (q.l_quantity >= QUANTITY1)
        & (q.l_quantity <= QUANTITY1 + 10)
        & (q.p_size.between(1, 5))
        & (q.l_shipmode.isin(("AIR", "AIR REG")))
        & (q.l_shipinstruct == "DELIVER IN PERSON")
    )

    q2 = (
        (q.p_brand == BRAND2)
        & (q.p_container.isin(("MED BAG", "MED BOX", "MED PKG", "MED PACK")))
        & (q.l_quantity >= QUANTITY2)
        & (q.l_quantity <= QUANTITY2 + 10)
        & (q.p_size.between(1, 10))
        & (q.l_shipmode.isin(("AIR", "AIR REG")))
        & (q.l_shipinstruct == "DELIVER IN PERSON")
    )

    q3 = (
        (q.p_brand == BRAND3)
        & (q.p_container.isin(("LG CASE", "LG BOX", "LG PACK", "LG PKG")))
        & (q.l_quantity >= QUANTITY3)
        & (q.l_quantity <= QUANTITY3 + 10)
        & (q.p_size.between(1, 15))
        & (q.l_shipmode.isin(("AIR", "AIR REG")))
        & (q.l_shipinstruct == "DELIVER IN PERSON")
    )

    q = q.filter([q1 | q2 | q3])
    q = q.aggregate(revenue=(q.l_extendedprice * (1 - q.l_discount)).sum())
    return q


def q20(supplier, nation, partsupp, part, lineitem, **kwargs):
    var1 = date(1994, 1, 1)
    var2 = date(1995, 1, 1)
    var3 = "CANADA"
    var4 = "forest"

    q1 = supplier.join(nation, supplier.s_nationkey == nation.n_nationkey)

    q3 = part.filter([part.p_name.like(f"{var4}%")])
    q2 = partsupp

    q4 = lineitem.filter(
        [
            lineitem.l_partkey == q2.ps_partkey,
            lineitem.l_suppkey == q2.ps_suppkey,
            lineitem.l_shipdate >= var1,
            lineitem.l_shipdate < var2,
        ]
    )

    q2 = q2.filter(
        [
            partsupp.ps_partkey.isin(q3.p_partkey),
            partsupp.ps_availqty > 0.5 * q4.l_quantity.sum(),
        ]
    )

    q1 = q1.filter([q1.n_name == var3, q1.s_suppkey.isin(q2.ps_suppkey)])

    q1 = q1[q1.s_name, q1.s_address]

    return q1.order_by(q1.s_name)


def q21(supplier, lineitem, orders, nation, **kwargs):
    NATION = "SAUDI ARABIA"

    L2 = lineitem.view()
    L3 = lineitem.view()

    q = supplier
    q = q.join(lineitem, supplier.s_suppkey == lineitem.l_suppkey)
    q = q.join(orders, orders.o_orderkey == lineitem.l_orderkey)
    q = q.join(nation, supplier.s_nationkey == nation.n_nationkey)
    q = q[
        q.l_orderkey.name("l1_orderkey"),
        q.o_orderstatus,
        q.l_receiptdate,
        q.l_commitdate,
        q.l_suppkey.name("l1_suppkey"),
        q.s_name,
        q.n_name,
    ]
    q = q.filter(
        [
            q.o_orderstatus == "F",
            q.l_receiptdate > q.l_commitdate,
            q.n_name == NATION,
            ((L2.l_orderkey == q.l1_orderkey) & (L2.l_suppkey != q.l1_suppkey)).any(),
            ~(
                (
                    (L3.l_orderkey == q.l1_orderkey)
                    & (L3.l_suppkey != q.l1_suppkey)
                    & (L3.l_receiptdate > L3.l_commitdate)
                ).any()
            ),
        ]
    )

    gq = q.group_by([q.s_name])
    q = gq.aggregate(numwait=q.count())
    q = q.order_by([ibis.desc(q.numwait), q.s_name])
    return q.limit(100)


def q22(customer, orders, **kwargs):
    COUNTRY_CODES = ("13", "31", "23", "29", "30", "18", "17")

    q = customer.filter(
        [
            customer.c_acctbal > 0.00,
            customer.c_phone.substr(0, 2).isin(COUNTRY_CODES),
        ]
    )
    q = q.aggregate(avg_bal=customer.c_acctbal.mean())

    custsale = customer.filter(
        [
            customer.c_phone.substr(0, 2).isin(COUNTRY_CODES),
            customer.c_acctbal > q.avg_bal,
            ~(orders.o_custkey == customer.c_custkey).any(),
        ]
    )
    custsale = custsale[
        customer.c_phone.substr(0, 2).name("cntrycode"), customer.c_acctbal
    ]

    gq = custsale.group_by(custsale.cntrycode)
    outerq = gq.aggregate(numcust=custsale.count(), totacctbal=custsale.c_acctbal.sum())

    return outerq.order_by(outerq.cntrycode)


all_queries = [
    q1,
    q2,
    q3,
    q4,
    q5,
    q6,
    q7,
    q8,
    q9,
    q10,
    q11,
    q12,
    q13,
    q14,
    q15,
    q16,
    q17,
    q18,
    q19,
    q20,
    q21,
    q22,
]
