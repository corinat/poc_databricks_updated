"""Tests for the DQX quality gate at the Bronze to Silver boundary.

Three things are checked here, and only the first needs no compute:

  test_checks_files_are_valid   the rule files are well-formed DQX metadata
  test_gate_splits_rows        the right rows land on the right side
  test_quarantine_projection   the quarantine payload is built correctly

The second and third build their input in memory and pass reference frames
rather than tables, so no table is read and none is written. That is the point:
proving the gate works should not mean inserting rows into bronze_order and
remembering to delete them.

Running these is optional. No requirement asks for tests, and nothing in the
deliverables depends on them. test_checks_files_are_valid is the one worth
having in CI — it puts all 46 rule definitions through DQX's own validator, so a
misspelled check function or a missing argument fails there rather than at the
next deploy, and it needs no Java, no compute and no pandas. The other two are a
developer convenience: they exercise the gate's behaviour, and they need a Spark
session.

The spark fixture below prefers a local session, falls back to Databricks
Connect, and skips the test if it can have neither; DQEngine is given a mock
workspace client because the checks never call the workspace. So with OSS
pyspark and a JDK installed, the whole file runs offline. On macOS, verified:

    JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home \
    uv run --no-project --with pyspark --with databricks-labs-dqx==0.16.0 \
        --with pytest --with pyyaml --with "pandas<3" \
        pytest tests/test_quality_gate.py -v

All three pass that way, in about a minute and a half, with no workspace and no
compute. Four things that command is particular about:

  JDK 17 or 21. Spark 4 does not run on a newer one: JDK 26 fails at startup
  with ClassNotFoundException on jdk.internal.ref.Cleaner, a class those JDKs
  no longer have.

  pandas, pinned below 3. DQX imports it in check_funcs without declaring it,
  and PySpark warns that it does not yet fully support pandas 3.

  No virtual environment active. `databricks` is a namespace package, so a
  VIRTUAL_ENV pointing at an environment that has databricks-connect leaves it
  on sys.path even under --no-project, where it collides with the pyspark
  installed alongside it.

  The dqx version pinned to the one databricks.yml installs on the jobs, so the
  rules are checked against the engine that will run them.

On Windows the same packages and the same JDK constraint apply, but the
invocation differs and may need more. JAVA_HOME is set by the shell rather than
inline — `$env:JAVA_HOME = "..."` in PowerShell, `set JAVA_HOME=...` in cmd —
and the path is wherever the JDK installed, not the macOS one above. Local Spark
on Windows may additionally want HADOOP_HOME and winutils.exe, which is a
Spark-on-Windows requirement rather than anything this project controls. None of
that was tested here. Anyone who would rather not find out can name compute in
the Databricks profile and let the two Spark tests run there instead, or leave
them skipped — the rule-file test runs on any platform regardless.

In the project's own environment databricks-connect replaces pyspark and refuses
local sessions, so the second and third tests need compute named in the
Databricks profile — `serverless_compute_id = auto`, a cluster_id, or
SPARK_REMOTE. Without it they skip and the first still runs:

    uv run pytest tests/test_quality_gate.py
    1 passed, 2 skipped

That first test is the one worth having in CI. It puts all 46 rule definitions
through DQX's own validator, so a misspelled check function or a missing
argument fails here rather than at the next deploy — something no amount of
sample data can catch, because data only exercises the rules it happens to
break.
"""

import datetime
import decimal
import pathlib
from unittest.mock import MagicMock

import pytest
import yaml
from databricks.labs.dqx.engine import DQEngine
from dq import RESULT_COLUMNS, quarantine_rows
from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, DecimalType, IntegerType, StringType,
                               StructField, StructType, TimestampType)

CHECKS_DIR = pathlib.Path(__file__).parent.parent / "checks"

# The shape silver_order's gate sees: the cleaned, cast, renamed frame, after
# the dedupe and before the write.
ORDER_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", StringType()),
    StructField("currency_code", StringType()),
    StructField("order_date", DateType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DecimalType(18, 2)),
    StructField("net_amount", DecimalType(18, 2)),
    StructField("status", StringType()),
    StructField("sales_channel", StringType()),
    StructField("source_file", StringType()),
    StructField("ingested_at", TimestampType()),
])


def _order(order_id, customer_id=101, product_id="P-001", currency_code="EUR",
        quantity=1, unit_price="49.00", net_amount="49.00", status="paid"):
    return (
        order_id, customer_id, product_id, currency_code,
        datetime.date(2026, 1, 8), quantity,
        decimal.Decimal(unit_price), decimal.Decimal(net_amount),
        status, None, "orders.parquet", datetime.datetime(2026, 1, 8, 12, 0, tzinfo=datetime.UTC),
    )


def _load(name):
    with open(CHECKS_DIR / name) as handle:
        return yaml.safe_load(handle)


def test_checks_files_are_valid():
    """Every rule file is structurally and semantically valid DQX metadata.

    Needs no Spark and no workspace. This is the cheap guard: a mistyped check
    function, a wrong argument name or two conflicting rules on one column are
    all caught here rather than at the first pipeline run.
    """
    files = sorted(CHECKS_DIR.glob("*.yml"))
    assert files, "no rule files found in checks/"

    for path in files:
        status = DQEngine.validate_checks(_load(path.name))
        assert not status.has_errors, f"{path.name}: {status.errors}"


@pytest.fixture(scope="module")
def spark():
    """A Spark session, local if this environment allows one.

    Overrides the session from tests/conftest.py. OSS pyspark gives a local
    session; databricks-connect replaces the builder and refuses master(), and
    then the only option is a remote one.

    If neither is available the tests using this fixture skip rather than
    error. The difference is how a run reads: an error says the project is
    broken, a skip says the test needs compute this machine has not been given.
    Nothing is skipped silently — the reason carries the underlying message.

    test_checks_files_are_valid does not take this fixture. It validates the
    rule files through DQX's own validator, which is ordinary Python, so it runs
    everywhere and is the test that guards all 46 rule definitions.
    """
    try:
        return (
            SparkSession.builder
            .master("local[*]")
            .appName("dqx-gate-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
    except Exception:
        pass

    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except Exception as exc:
        pytest.skip(
            "no Spark session available, so the gate's behaviour is not "
            f"exercised here: {exc}"
        )


@pytest.fixture()
def gate_result(spark):
    """Run silver_order's real rules over four hand-built rows."""
    rows = [
        _order("O-0001"),                                   # clean
        _order("O-0002", quantity=0),                       # error: quantity
        _order("O-0003", net_amount="-5.00"),               # error: net_amount
        _order("O-0004", status="unknown"),                 # warn only
        _order("O-0005", currency_code="XXX"),               # error: currency
    ]
    orders = spark.createDataFrame(rows, ORDER_SCHEMA)

    # Reference frames, not tables. The foreign key rules only need something
    # with the right column, so the gate is testable without silver_customer,
    # bronze_product or bronze_currency existing anywhere.
    ref_dfs = {
        "silver_customer": spark.createDataFrame([(101,)], "customer_id int"),
        "bronze_product": spark.createDataFrame([("P-001",)], "product_id string"),
        "bronze_currency": spark.createDataFrame([("EUR",)], "currency_code string"),
    }

    # MagicMock, not a real client: DQEngine only uses it to verify
    # connectivity at construction, and nothing here touches the workspace.
    valid, flagged = DQEngine(MagicMock()).apply_checks_by_metadata_and_split(
        orders, _load("silver_order.yml"), ref_dfs=ref_dfs
    )
    return valid, flagged


def test_gate_splits_rows(gate_result):
    """error keeps a row out of Silver; warn lets it through and records it."""
    valid, flagged = gate_result

    in_silver = {r.order_id for r in valid.collect()}
    in_quarantine = {r.order_id for r in flagged.collect()}

    # The clean row passes and is not recorded.
    assert "O-0001" in in_silver
    assert "O-0001" not in in_quarantine

    # The two error rows are kept out of Silver entirely.
    assert "O-0002" not in in_silver, "a non-positive quantity reached Silver"
    assert "O-0003" not in in_silver, "a negative net_amount reached Silver"
    assert {"O-0002", "O-0003"} <= in_quarantine

    # The warn-only row is in both: an unfamiliar status is recorded but is not
    # a reason to lose the order.
    assert "O-0004" in in_silver
    assert "O-0004" in in_quarantine

    # An unknown currency is an error, not a warning: without a rate the order
    # cannot be converted, so it is kept out of Silver rather than sitting in
    # Gold with a NULL where a converted amount belongs.
    assert "O-0005" not in in_silver, "an unconvertible currency reached Silver"
    assert "O-0005" in in_quarantine


def _payload(row):
    """The quarantined row's VARIANT payload, as a dict.

    collect() returns a VARIANT column as a VariantVal, which is not a mapping;
    toPython() is what turns it into one.
    """
    return row.row_data.toPython()


def test_quarantine_projection(gate_result):
    """The quarantine payload carries the row, the severity and the reasons."""
    _, flagged = gate_result
    rows = {
        r.source_table + ":" + _payload(r)["order_id"]: r
        for r in quarantine_rows(flagged, "bronze_order").collect()
    }

    error_row = rows["bronze_order:O-0002"]
    warn_row = rows["bronze_order:O-0004"]

    assert error_row.severity == "error"
    assert warn_row.severity == "warn"

    # The reasons name the rules that fired.
    assert "quantity_is_not_positive" in {c.name for c in error_row.failed_checks}
    assert "status_is_not_in_the_list" in {c.name for c in warn_row.failed_checks}
    assert all(c.message for c in error_row.failed_checks)

    # The payload is the row, not the report.
    payload_keys = set(_payload(warn_row))
    assert "net_amount" in payload_keys
    assert "source_file" in payload_keys
    assert not payload_keys & set(RESULT_COLUMNS)
