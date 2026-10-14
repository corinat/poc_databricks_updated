# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 06, step 5: the three checks the requirements ask for, as the last task
# of the mart job.
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
# All three are evaluated before anything is raised, so one run reports every
# failure it found instead of stopping at the first and hiding the rest.
#
# Orphan foreign keys are deliberately not checked here. A customer_id or
# seller_id absent from its dimension is a completeness question, and the DQX
# gates already report it as a warning at the Bronze to Silver boundary where
# the row enters. Failing the Gold publish for it would make a warning fatal
# two layers later.

# COMMAND ----------
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

# ── Fail the job if any check failed ──
# One exception carrying every failure, so a run does not have to be repeated
# to discover the second problem.
if failures:
    raise AssertionError(
        f"{len(failures)} mart check(s) failed:\n- " + "\n- ".join(failures)
    )
