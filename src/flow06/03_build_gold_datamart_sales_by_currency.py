# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 06, step 3: sales per day with one column per currency.
#
# - Grain: one row per date, as section 4 specifies.
# - Built with the three operations section 4 names: groupBy, pivot and
#   withColumn. The DataFrame API rather than SQL, because pivot is where the
#   requirement is.
# - Write mode: full overwrite, replacing the schema.
#
# Source: gold_datamart_daily_sales_summary, not the detail mart.
#
# Section 4 writes the grouping column as order_date, which is the detail
# mart's name for it, but reading the summary instead is what keeps the two
# sinks consistent: the summary excludes cancelled orders, and a sink built
# from the detail mart would include them, so the same day would carry two
# different sales figures depending on which table the dashboard asked. The
# column is named sales_date here, as the summary and the other sink name it.
#
# Which measure is pivoted, and why it has to be sales_pln.
#
# The requirement asks for a total across currencies. Summing EUR, GBP and USD
# amounts into one number is not arithmetic — it adds quantities in different
# units. sales_pln is already converted, so every pivoted value is in the same
# unit and the total means something. That is also what the lit() column below
# records: currency_base = 'PLN' says what unit the whole row is in.
#
# The pivot's column list comes from dim_currency.
#
# Passing the list explicitly makes the shape depend on the approved currency
# reference data rather than on which currencies happen to have sold. Without
# it, pivot runs its own query to discover the distinct values, and a day with
# no USD orders anywhere in the data would silently drop the USD column. An
# approved currency with no sales gets a column of NULLs instead, which is the
# honest answer.
#
# This is the one Gold table whose column set is not declared in DDL. Section 4
# asks for one column per currency, so the shape follows the reference data by
# definition: adding a currency to dim_currency adds a column here. The write
# therefore replaces the schema rather than inserting into a declared one.
#
# NULL and zero are different here.
#
# A currency column is NULL for a date with no non-cancelled orders in that
# currency, not 0. The project keeps that distinction elsewhere — an order with
# no published rate gets a NULL order_amount_pln rather than falling back to
# its net amount — and it is the difference between "nothing sold" and "sold,
# summing to nothing". The total treats a NULL as 0, so it stays complete.

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

summary_table = f"{catalog}.{gold_schema}.gold_datamart_daily_sales_summary"
dim_currency = f"{catalog}.{gold_schema}.dim_currency"

table_name = f"{catalog}.{gold_schema}.gold_datamart_sales_by_currency"

# COMMAND ----------

# ── The approved currencies, in a fixed order ──
# Ordered so the column order is stable across runs rather than following
# whatever order the dimension happens to return.
currencies = [
    row.currency_code
    for row in spark.table(dim_currency)
    .select("currency_code")
    .distinct()
    .orderBy("currency_code")
    .collect()
]

# COMMAND ----------

# ── group by, pivot ──
# One row per date, one column per approved currency, each holding that day's
# PLN sales in it.
pivoted = (
    spark.table(summary_table)
    .groupBy("sales_date")
    .pivot("currency_code", currencies)
    .agg(F.sum("sales_pln"))
)

# sum over DECIMAL(18,2) widens to DECIMAL(28,2); the money columns are brought
# back to the scale the rest of Gold uses.
for currency in currencies:
    pivoted = pivoted.withColumn(currency, F.col(currency).cast("decimal(18,2)"))

# COMMAND ----------

# ── withColumn: the total across currencies, and the unit it is in ──
# coalesce inside the sum, so a currency with no sales that day contributes 0
# to the total instead of making the whole total NULL.
#
# currency_base is the literal column section 4 asks for: a value no source
# column carries. It is not a marker for its own sake — the pivoted columns are
# named after the currency the orders were placed in, while the amounts in them
# are converted, and this column is what says so.
total_pln = reduce(
    lambda left, right: left + right,
    [F.coalesce(F.col(currency), F.lit(0)) for currency in currencies],
)

sales_by_currency = (
    pivoted
    .withColumn("total_pln", total_pln.cast("decimal(18,2)"))
    .withColumn("currency_base", F.lit("PLN"))
    .orderBy("sales_date")
)

# COMMAND ----------

# ── Publish ──
# overwriteSchema, because the column set follows dim_currency: a currency
# added to the reference data adds a column, and the write has to be allowed to
# change the table's shape for that to work.
(
    sales_by_currency
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)
