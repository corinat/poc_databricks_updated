# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 06, step 5: the three checks the requirements ask for and two
# reconciliations between the marts, as the last task of the mart job.
#
# They live in a task of their own rather than at the end of the notebooks that
# write the tables. A check at the end of a build runs after the write has
# already committed, so it can only report a table it has no power to stop;
# here, a failure fails the job, and flow 07's dashboard refresh and alerts —
# which depend on this job — do not run against numbers that did not add up.
#
# The three:
#
#   1. The grain of the joined mart, section 3: "re-check the grain stays one
#      row per order_id after all joins", and "confirm cardinality per join".
#      Both are the same question asked two ways — did any of the six joins
#      multiply rows — so the check compares three counts that must agree:
#      the mart's rows, its distinct order ids, and fact_order's rows.
#
#      A join that fanned out makes the first exceed the other two. A join
#      written as inner by mistake makes the first fall short of the third.
#
#   2. The currency codes, section 4: "use distinct only to validate inputs
#      (e.g. currency_code values match dim_currency) — not as a table of its
#      own". distinct is used here and the result is never written anywhere,
#      which is the distinction the requirement draws. An unapproved currency
#      matters beyond tidiness: the conversion to PLN is only defined for the
#      currencies the reference data approves.
#
#   3. The reconciliation, section 4: SUM(metric_value) for net_sales in
#      gold_datamart_daily_metrics against SUM(net_sales) in
#      gold_datamart_daily_sales_summary. The unpivot must not have lost or
#      duplicated a measure.
#
# Two more, which the requirements do not ask for. Each covers a decision that
# a mart makes and no other check reaches, and each compares two tables rather
# than restating the rule the mart already applies:
#
#   4. The summary's exclusion of cancelled orders. The summary is the only
#      place that exclusion is applied, so nothing else would notice it going
#      missing: every total would simply rise. Summing the detail mart under
#      the same condition is an independent way to arrive at the figure the
#      summary should hold.
#
#   5. The pivot, two ways. total_pln has to equal the currency columns it is
#      built from, which catches that expression drifting from the column list
#      when a currency is approved. And the pivot's total has to equal the
#      summary's sales_pln, which catches a currency present in the summary
#      but missing from dim_currency: the pivot names its columns from the
#      dimension, so such a currency is dropped silently, and the per-row
#      comparison cannot see it because total_pln is computed from the columns
#      that remain.
#
# All five are evaluated before anything is raised, so one run reports every
# failure it found instead of stopping at the first and hiding the rest.
#
# Orphan foreign keys are deliberately not checked here. A customer_id or
# seller_id absent from its dimension is a completeness question, and the DQX
# gates already report it as a warning at the Bronze to Silver boundary where
# the row enters. Failing the Gold publish for it would make a warning fatal
# two layers later.

# COMMAND ----------
from functools import reduce

from pyspark.sql import functions as F

# COMMAND ----------
# ── Parameters (passed by DABs job as job-level parameters) ──
# DABs dev mode prefixes schema names with dev_<username>_.
# Compute the correct default so interactive runs match the bundle.
_user = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]
_dev_gold_schema = f"dev_{_user}_gold"

dbutils.widgets.text("catalog", "poc_dev")
dbutils.widgets.text("gold_schema", _dev_gold_schema)

catalog = dbutils.widgets.get("catalog")
gold_schema = dbutils.widgets.get("gold_schema")

detail_table = f"{catalog}.{gold_schema}.gold_datamart_order_sales_detail"
summary_table = f"{catalog}.{gold_schema}.gold_datamart_daily_sales_summary"
metrics_table = f"{catalog}.{gold_schema}.gold_datamart_daily_metrics"
by_currency_table = f"{catalog}.{gold_schema}.gold_datamart_sales_by_currency"
fact_order = f"{catalog}.{gold_schema}.fact_order"
dim_currency = f"{catalog}.{gold_schema}.dim_currency"

failures = []

# COMMAND ----------

# ── 1. Grain and cardinality ──
detail = spark.table(detail_table)

detail_rows = detail.count()
distinct_orders = detail.select("order_id").distinct().count()
order_rows = spark.table(fact_order).count()

if not detail_rows == distinct_orders == order_rows:
    failures.append(
        "grain: gold_datamart_order_sales_detail has "
        f"{detail_rows} rows, {distinct_orders} distinct order_id and "
        f"fact_order has {order_rows} rows; all three must be equal. "
        "More rows than distinct ids means a join fanned out; fewer rows "
        "than fact_order means a join dropped orders."
    )

# COMMAND ----------

# ── 2. Currency codes against the reference data ──
# distinct as a validation, not as a table: the result is compared and
# discarded.
mart_currencies = {
    row.currency_code
    for row in detail.select("currency_code").distinct().collect()
}
approved_currencies = {
    row.currency_code
    for row in spark.table(dim_currency).select("currency_code").distinct().collect()
}

unapproved = sorted(mart_currencies - approved_currencies)
if unapproved:
    failures.append(
        f"currency: {unapproved} appear in gold_datamart_order_sales_detail "
        "but not in dim_currency, so their conversion to PLN is undefined"
    )

# COMMAND ----------

# ── 3. The unpivot reconciles with its source ──
# Both sides are DECIMAL, so the comparison is exact rather than approximate.
# A table with no rows sums to NULL, which coalesce turns into 0 so that two
# empty tables reconcile instead of comparing NULL to NULL.
metrics_net_sales = (
    spark.table(metrics_table)
    .filter(F.col("metric_name") == "net_sales")
    .agg(F.coalesce(F.sum("metric_value"), F.lit(0)).alias("total"))
    .collect()[0]
    .total
)
summary_net_sales = (
    spark.table(summary_table)
    .agg(F.coalesce(F.sum("net_sales"), F.lit(0)).alias("total"))
    .collect()[0]
    .total
)

if metrics_net_sales != summary_net_sales:
    failures.append(
        "reconciliation: net_sales sums to "
        f"{metrics_net_sales} in gold_datamart_daily_metrics and "
        f"{summary_net_sales} in gold_datamart_daily_sales_summary; "
        "the unpivot lost or duplicated rows"
    )

# COMMAND ----------

# ── 4. The summary excludes cancelled orders ──
# The same condition the summary applies, evaluated against the detail mart,
# which carries one row per order and so can be summed independently.
#
# A NULL order_status is excluded on both sides: SQL's <> and the filter below
# both drop it, so the two figures agree on such a row rather than disagreeing
# about how to treat it.
detail_net_sales = (
    detail
    .filter(F.col("order_status") != "cancelled")
    .agg(F.coalesce(F.sum("net_amount"), F.lit(0)).alias("total"))
    .collect()[0]
    .total
)

if detail_net_sales != summary_net_sales:
    failures.append(
        "cancelled orders: net_amount over gold_datamart_order_sales_detail "
        f"excluding cancelled orders sums to {detail_net_sales}, while "
        f"net_sales in gold_datamart_daily_sales_summary sums to "
        f"{summary_net_sales}; the summary is including orders it should "
        "leave out, or excluding ones it should keep"
    )

# COMMAND ----------

# ── 5. The pivot reconciles with its own columns and with its source ──
# The currency columns are named after the approved currencies, so they are
# discovered from the table rather than listed here: the set changes whenever a
# currency is approved, and a list here would have to be kept in step with
# dim_currency to go on meaning anything.
by_currency = spark.table(by_currency_table)
currency_columns = [
    column
    for column in by_currency.columns
    if column not in {"sales_date", "total_pln", "currency_base"}
]

# Guarded because reduce over an empty list raises. No approved currency means
# no currency column, and the pivot that built this table would have failed
# first; the guard keeps this check from being the thing that reports it.
if currency_columns:
    row_total = reduce(
        lambda left, right: left + right,
        [F.coalesce(F.col(column), F.lit(0)) for column in currency_columns],
    )
    inconsistent_rows = by_currency.filter(
        F.col("total_pln") != row_total.cast("decimal(18,2)")
    ).count()

    if inconsistent_rows:
        failures.append(
            f"pivot total: {inconsistent_rows} row(s) of "
            "gold_datamart_sales_by_currency have a total_pln that is not the "
            f"sum of {currency_columns}; the total no longer covers every "
            "currency column it is built from"
        )

# Both sides aggregate the same measure, so they are equal whatever the pivot
# groups by. They part company when a currency reaches the summary without
# being in dim_currency: the pivot has no column to put it in and drops it.
pivot_pln = (
    by_currency
    .agg(F.coalesce(F.sum("total_pln"), F.lit(0)).alias("total"))
    .collect()[0]
    .total
)
summary_pln = (
    spark.table(summary_table)
    .agg(F.coalesce(F.sum("sales_pln"), F.lit(0)).alias("total"))
    .collect()[0]
    .total
)

if pivot_pln != summary_pln:
    failures.append(
        f"pivot coverage: total_pln sums to {pivot_pln} in "
        "gold_datamart_sales_by_currency and sales_pln sums to "
        f"{summary_pln} in gold_datamart_daily_sales_summary; the pivot is "
        "dropping a currency the summary reports, which happens when it is "
        "missing from dim_currency"
    )

# COMMAND ----------

# ── Fail the job if any check failed ──
# One exception carrying every failure, so a run does not have to be repeated
# to discover the second problem.
if failures:
    raise AssertionError(
        f"{len(failures)} mart check(s) failed:\n- " + "\n- ".join(failures)
    )
