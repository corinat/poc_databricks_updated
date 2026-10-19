"""Tests for the two transformations in src/common/transforms.py.

Four tests, chosen for one reason each: they cover logic that either never
executes on the sample data, or produces a column that nothing downstream and
no DQX rule would notice going wrong.

  test_hierarchy_levels_and_paths     the recursive walk. level, hierarchy_path
                                      and root_customer_id are checked by no
                                      rule and reach no Gold table, so a
                                      regression there is invisible everywhere
                                      else.
  test_rate_is_the_latest_on_or_before   the rate join matches on a range, but
                                      the sample data has a rate for every
                                      order date, so the fallback to an earlier
                                      date never runs.
  test_reporting_currency_converts_at_one   there are no PLN orders in the
                                      sample data, so the second branch of the
                                      coalesce never runs.
  test_one_invoice_per_order          nothing in the data gives an order two
                                      invoices, and the row_number is the only
                                      thing keeping the mart's grain.

Deliberately not here: anything the 45 DQX rules already assert, the three
checks flow 06's check_marts runs against real data on every job run, the
dimension and fact projections whose shape is pinned in DDL, and the summary,
pivot and unpivot, whose output is in the row counts each README documents.

Running these needs a Spark session. The simplest source is the workspace:
put `serverless_compute_id = auto` in the Databricks profile and
tests/conftest.py opens the session for you — that is what the bundle
template's conftest is for. Then:

    uv run pytest tests/test_transforms.py -v

No JDK, no local Spark. A local session works too, with OSS pyspark and a JDK
17 or 21, but needs an environment where databricks-connect does not shadow
pyspark.
"""

import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from transforms import (
    cleaned_orders,
    combined_rates,
    customer_hierarchy,
    order_sales_detail,
)


@pytest.fixture(scope="module")
def spark():
    """A Spark session, local if this environment allows one.

    Overrides the session from tests/conftest.py, so the file runs in either
    environment: a local session where OSS pyspark and a JDK are available, and
    Databricks Connect where compute is named in the profile. If neither is
    available the tests skip rather than error — the difference is how a run
    reads, since an error says the project is broken while a skip says the test
    needs compute this machine has not been given.
    """
    try:
        return (
            SparkSession.builder
            .master("local[*]")
            .appName("transform-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
    except Exception:
        pass

    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except Exception as exc:
        pytest.skip(f"no Spark session available: {exc}")


def _frame(spark, rows, schema: str):
    """Rows as a DataFrame. Three of the four transformations take frames."""
    return spark.createDataFrame(list(rows), schema=schema)


def _view(spark, name: str, rows: list[dict], schema: str):
    """Register rows as a temporary view and return its name.

    Only customer_hierarchy needs this: it is the one transformation written in
    SQL, because a recursive CTE has no DataFrame-API form, so it takes a name
    rather than a frame.
    """
    spark.createDataFrame(rows, schema=schema).createOrReplaceTempView(name)
    return name


# ── The customer hierarchy ──


def test_hierarchy_levels_and_paths(spark):
    """Three generations, and an orphan whose parent is not in the data."""
    source = _view(
        spark,
        "t_bronze_customer",
        [
            # a holding company, its account, and a branch under that
            {"customer_id": "1", "customer_name": "Holding", "country": "PL",
            "parent_customer_id": None, "source_file": "a.csv", "ingested_at": dt.datetime(2026, 1, 1)},
            {"customer_id": "2", "customer_name": "Account", "country": "PL",
            "parent_customer_id": "1", "source_file": "a.csv", "ingested_at": dt.datetime(2026, 1, 1)},
            {"customer_id": "3", "customer_name": "Branch", "country": "DE",
            "parent_customer_id": "2", "source_file": "a.csv", "ingested_at": dt.datetime(2026, 1, 1)},
            # parent 99 is not in the data, so this row starts a tree of its own
            {"customer_id": "4", "customer_name": "Orphan", "country": "US",
            "parent_customer_id": "99", "source_file": "a.csv", "ingested_at": dt.datetime(2026, 1, 1)},
        ],
        "customer_id string, customer_name string, country string, "
        "parent_customer_id string, source_file string, ingested_at timestamp",
    )

    rows = {
        r.customer_id: r
        for r in customer_hierarchy(spark, source, "country").collect()
    }

    assert len(rows) == 4, "every customer keeps a row, including the orphan"

    assert (rows[1].level, rows[1].hierarchy_path, rows[1].root_customer_id) == (
        0, "Holding", 1)
    assert (rows[2].level, rows[2].hierarchy_path, rows[2].root_customer_id) == (
        1, "Holding > Account", 1)
    assert (rows[3].level, rows[3].hierarchy_path, rows[3].root_customer_id) == (
        2, "Holding > Account > Branch", 1)

    # The orphan starts its own tree rather than disappearing, and stays
    # identifiable: level 0 with a parent_customer_id that is set.
    assert rows[4].level == 0
    assert rows[4].root_customer_id == 4
    assert rows[4].parent_customer_id == 99

    # The country column is reconciled and upper-cased.
    assert rows[3].country_code == "DE"


def test_hierarchy_keeps_the_newest_batch(spark):
    """One row per customer_id, and the later delivery is the one kept."""
    source = _view(
        spark,
        "t_bronze_customer_dupes",
        [
            {"customer_id": "1", "customer_name": "Old name", "country": "PL",
            "parent_customer_id": None, "source_file": "first.csv",
            "ingested_at": dt.datetime(2026, 1, 1)},
            {"customer_id": "1", "customer_name": "New name", "country": "PL",
            "parent_customer_id": None, "source_file": "second.csv",
            "ingested_at": dt.datetime(2026, 1, 2)},
        ],
        "customer_id string, customer_name string, country string, "
        "parent_customer_id string, source_file string, ingested_at timestamp",
    )

    rows = customer_hierarchy(spark, source, "country").collect()

    assert len(rows) == 1
    assert rows[0].customer_name == "New name"
    assert rows[0].source_file == "second.csv"


# ── The joined mart ──

_ORDER_SCHEMA = (
    "order_id string, customer_id int, product_id string, currency_code string, "
    "order_date date, quantity int, unit_price decimal(18,2), "
    "net_amount decimal(18,2), status string"
)
_RATE_SCHEMA = (
    "rate_date date, currency_code string, rate_to_pln decimal(18,6), "
    "rate_source string, table_type string"
)
_INVOICE_SCHEMA = (
    "invoice_id string, order_id string, seller_id string, currency_code string, "
    "invoice_date date, net_amount decimal(18,2), vat_amount decimal(18,2), "
    "gross_amount decimal(18,2), pdf_path string, invoice_status string"
)


_DEFAULT_CURRENCIES = [
    {"currency_code": "EUR", "currency_name": "Euro",
     "decimal_places": 2, "is_reporting_currency": False},
    {"currency_code": "PLN", "currency_name": "Polish zloty",
     "decimal_places": 2, "is_reporting_currency": True},
]


def _detail(spark, orders, rates, invoices=(), currencies=None):
    """Run order_sales_detail over frames this call builds, keyed by order_id.

    Each test supplies only the rows it cares about; the dimensions default to
    one row each, enough for the left joins to resolve.
    """
    detail = order_sales_detail(
        fact_order=_frame(spark, orders, _ORDER_SCHEMA),
        fact_invoice=_frame(spark, invoices, _INVOICE_SCHEMA),
        fact_exchange_rate=_frame(spark, rates, _RATE_SCHEMA),
        dim_customer=_frame(
            spark,
            [{"customer_id": 1, "customer_name": "C", "customer_type": None,
              "country_code": "PL", "parent_customer_id": None}],
            "customer_id int, customer_name string, customer_type string, "
            "country_code string, parent_customer_id int"),
        dim_product=_frame(
            spark,
            [{"product_id": "P-001", "product_name": "P", "category": "apparel",
              "list_price": Decimal("10.00"), "unit_of_measure": None}],
            "product_id string, product_name string, category string, "
            "list_price decimal(18,2), unit_of_measure string"),
        dim_currency=_frame(
            spark,
            currencies if currencies is not None else _DEFAULT_CURRENCIES,
            "currency_code string, currency_name string, "
            "decimal_places int, is_reporting_currency boolean"),
        dim_seller=_frame(
            spark,
            [{"seller_id": "SEL-001", "seller_name": "S", "tax_id": "1",
              "address": "a", "postal_code": "1", "city": "c",
              "country": "PL", "currency_code": "PLN"}],
            "seller_id string, seller_name string, tax_id string, "
            "address string, postal_code string, city string, "
            "country string, currency_code string"),
    )
    return {r.order_id: r for r in detail.collect()}


def _order(order_id, currency, order_date, net="100.00", status="paid"):
    return {
        "order_id": order_id, "customer_id": 1, "product_id": "P-001",
        "currency_code": currency, "order_date": dt.date.fromisoformat(order_date),
        "quantity": 1, "unit_price": Decimal(net), "net_amount": Decimal(net),
        "status": status,
    }


def _rate(currency, rate_date, rate):
    return {
        "rate_date": dt.date.fromisoformat(rate_date), "currency_code": currency,
        "rate_to_pln": Decimal(rate), "rate_source": "NBP_API", "table_type": "A",
    }


def test_rate_is_the_latest_on_or_before(spark):
    """Not the nearest in either direction, and nothing from the future."""
    rows = _detail(
        spark,
        orders=[
            # falls between two published rates: the earlier one applies
            _order("O-1", "EUR", "2026-01-04"),
            # dated before the first published rate: no rate applies at all
            _order("O-2", "EUR", "2026-01-01"),
            # exactly on a rate date
            _order("O-3", "EUR", "2026-01-05"),
        ],
        rates=[
            _rate("EUR", "2026-01-02", "4.00"),
            _rate("EUR", "2026-01-05", "5.00"),
        ],
    )

    # 01-05 is nearer to 01-04 than 01-02 is, so a nearest-in-any-direction
    # match would pick 5.00. A transaction is not valued at a rate published
    # after it happened.
    assert rows["O-1"].rate_to_pln == Decimal("4.000000")
    assert rows["O-1"].order_amount_pln == Decimal("400.00")

    # No earlier rate exists, so the order keeps its row with nothing converted
    # rather than silently falling back to the net amount.
    assert rows["O-2"].rate_to_pln is None
    assert rows["O-2"].order_amount_pln is None

    assert rows["O-3"].rate_to_pln == Decimal("5.000000")


def test_reporting_currency_converts_at_one(spark):
    """A PLN order needs no published rate, and must not lose its PLN amount."""
    rows = _detail(
        spark,
        orders=[_order("O-1", "PLN", "2026-01-04", net="250.00")],
        rates=[_rate("EUR", "2026-01-02", "4.00")],  # nothing for PLN, ever
    )

    assert rows["O-1"].rate_to_pln == Decimal("1.000000")
    assert rows["O-1"].order_amount_pln == Decimal("250.00")


def test_one_invoice_per_order(spark):
    """Two invoices for one order leave one row, and the later one wins."""
    rows = _detail(
        spark,
        orders=[_order("O-1", "EUR", "2026-01-04")],
        rates=[_rate("EUR", "2026-01-02", "4.00")],
        invoices=[
            {"invoice_id": "INV-1", "order_id": "O-1", "seller_id": "SEL-001",
            "currency_code": "EUR", "invoice_date": dt.date(2026, 1, 5),
            "net_amount": Decimal("100.00"), "vat_amount": Decimal("23.00"),
            "gross_amount": Decimal("123.00"), "pdf_path": "a.pdf",
            "invoice_status": "issued"},
            {"invoice_id": "INV-2", "order_id": "O-1", "seller_id": "SEL-001",
            "currency_code": "EUR", "invoice_date": dt.date(2026, 1, 9),
            "net_amount": Decimal("100.00"), "vat_amount": Decimal("99.00"),
            "gross_amount": Decimal("199.00"), "pdf_path": "b.pdf",
            "invoice_status": "issued"},
        ],
    )

    assert len(rows) == 1, "the grain is one row per order, whatever the joins do"
    assert rows["O-1"].invoice_id == "INV-2"
    assert rows["O-1"].vat_amount == Decimal("99.00")


# ── Which delivery of a replayed order survives ──

_BRONZE_ORDER_SCHEMA = (
    "order_id string, customer_id string, product_id string, order_date string, "
    "quantity string, unit_price string, currency string, order_amount string, "
    "status string, sales_channel string, source_file string, ingested_at timestamp"
)


def _bronze_order(order_id, source_file, ingested_at, sales_channel=None,
                  status="paid"):
    return {
        "order_id": order_id, "customer_id": "1", "product_id": "P-001",
        "order_date": "2026-01-04", "quantity": "2", "unit_price": "10.00",
        "currency": "EUR", "order_amount": "20.00", "status": status,
        "sales_channel": sales_channel, "source_file": source_file,
        "ingested_at": ingested_at,
    }


def test_newest_delivery_of_an_order_wins(spark):
    """The schema-change delivery replays orders, and must be the copy kept.

    That is what gives a replayed order its sales_channel. The column stops at
    Silver and appears in no rule, so reversing the sort would empty it on
    every row with nothing failing.
    """
    orders = spark.createDataFrame(
        [
            # the batch file, first and without the widened column
            _bronze_order("O-1", "orders.parquet", dt.datetime(2026, 2, 1, 10, 0)),
            # the schema-change delivery replays it, later, carrying sales_channel
            _bronze_order("O-1", "orders_schema_change.parquet",
                          dt.datetime(2026, 2, 1, 12, 0), sales_channel="mobile"),
        ],
        schema=_BRONZE_ORDER_SCHEMA,
    )

    rows = cleaned_orders(orders).collect()

    assert len(rows) == 1, "one row per order_id, whatever the deliveries repeat"
    assert rows[0].source_file == "orders_schema_change.parquet"
    assert rows[0].sales_channel == "mobile"

    # The rename and the casts happen here too: Bronze carries strings.
    assert rows[0].currency_code == "EUR"
    assert rows[0].net_amount == Decimal("20.00")
    assert rows[0].quantity == 2


def test_status_is_lowercased_and_trimmed(spark):
    """Status feeds a DQX allowed-list rule, so its shape is load-bearing."""
    orders = spark.createDataFrame(
        [_bronze_order("O-1", "orders.parquet", dt.datetime(2026, 2, 1), status="  PAID ")],
        schema=_BRONZE_ORDER_SCHEMA,
    )

    assert cleaned_orders(orders).collect()[0].status == "paid"


# ── Which rate source wins a date both of them carry ──

_RATE_SOURCE_COLUMNS = [
    "table", "currency", "code", "no", "effectiveDate", "mid",
    "rate_source", "source_file", "ingested_at",
]
_BRONZE_RATE_SCHEMA = (
    "`table` string, currency string, code string, no string, "
    "effectiveDate date, mid decimal(18,6), source_file string, "
    "ingested_at timestamp"
)


def _bronze_rate(code, effective_date, mid, source_file, ingested_at):
    return {
        "table": "A", "currency": "euro", "code": code,
        "no": "1", "effectiveDate": dt.date.fromisoformat(effective_date),
        "mid": Decimal(mid), "source_file": source_file,
        "ingested_at": ingested_at,
    }


def test_api_wins_a_date_both_sources_carry(spark):
    """The bank's own publication beats the historical copy, and says so."""
    current = spark.createDataFrame(
        [_bronze_rate("EUR", "2026-01-02", "4.50", "api", dt.datetime(2026, 1, 2))],
        schema=_BRONZE_RATE_SCHEMA,
    )
    hist = spark.createDataFrame(
        [
            # same date and code as the API row: the API must win
            _bronze_rate("EUR", "2026-01-02", "9.99", "hist.csv", dt.datetime(2026, 1, 2)),
            # a date only the CSV has: it survives untouched
            _bronze_rate("EUR", "2026-01-01", "4.20", "hist.csv", dt.datetime(2026, 1, 1)),
        ],
        schema=_BRONZE_RATE_SCHEMA,
    )

    rows = {
        r.effectiveDate.isoformat(): r
        for r in combined_rates(current, hist, _RATE_SOURCE_COLUMNS).collect()
    }

    assert len(rows) == 2, "one row per effectiveDate and code"

    # Reversing the priority would put 9.99 here and label it HISTORICAL_CSV,
    # and nothing downstream would object: no rule mentions rate_source.
    assert rows["2026-01-02"].mid == Decimal("4.500000")
    assert rows["2026-01-02"].rate_source == "NBP_API"

    assert rows["2026-01-01"].mid == Decimal("4.200000")
    assert rows["2026-01-01"].rate_source == "HISTORICAL_CSV"
