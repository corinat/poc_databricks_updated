# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 06, step 4: the summary's five measures, one per row.
#
# - Source: gold_datamart_daily_sales_summary.
# - Grain: one row per sales_date + currency_code + product_category +
#   metric_name, as section 4 specifies. Five rows out of every summary row.
# - Built with unpivot, which is the operation section 4 names.
# - Write mode: full overwrite.
#
# Why the measures are cast before the unpivot.
#
# unpivot collapses several columns into one, so all of them have to share a
# type. order_count and item_quantity are INT and the three money measures are
# DECIMAL(18,2); left alone, Spark would find a common type on its own. They
# are cast explicitly instead, so metric_value's type is a decision in this
# notebook rather than a consequence of which measures happen to be listed.
# DECIMAL(18,2) holds both: a count is a whole number in it.
#
# What this shape is for, and what it costs.
#
# One row per measure is what lets a dashboard put every measure through the
# same filter and the same axis without a column per measure. The cost is that
# metric_value has no unit: a row holding 4 items and a row holding 255.98 PLN
# look alike, and only metric_name tells them apart. Nothing downstream may sum
# across metric_name.
#
# The column order follows the summary's, so METRIC_NAMES below reads in the
# order DATA_MODEL declares the measures.

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

summary_table = f"{catalog}.{gold_schema}.gold_datamart_daily_sales_summary"
table_name = f"{catalog}.{gold_schema}.gold_datamart_daily_metrics"

# The grain of the summary, carried through unchanged.
KEY_COLUMNS = ["sales_date", "currency_code", "product_category"]

# The five measures, in the order DATA_MODEL declares them.
METRIC_NAMES = [
    "order_count", "item_quantity", "net_sales", "gross_sales", "sales_pln",
]

# COMMAND ----------

# ── Declared schema ──
# The four key columns are NOT NULL: they are the grain. metric_name is part of
# it, so a measure that lost its name would fail the insert rather than land as
# an unidentifiable row.
# Clustered on the date and the metric name, which is how a dashboard panel
# reads this table: one measure over a date range.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    sales_date       DATE NOT NULL,
    currency_code    STRING NOT NULL,
    product_category STRING NOT NULL,
    metric_name      STRING NOT NULL,
    metric_value     DECIMAL(18,2)
)
CLUSTER BY (sales_date, metric_name)
""")

# COMMAND ----------

# ── Cast the measures to one type, then unpivot ──
# ids are the grain, values are the five measures, and the two new columns are
# named as section 4 asks: metric_name and metric_value.
summary = spark.table(summary_table)

for metric in METRIC_NAMES:
    summary = summary.withColumn(metric, F.col(metric).cast("decimal(18,2)"))

daily_metrics = summary.unpivot(
    ids=KEY_COLUMNS,
    values=METRIC_NAMES,
    variableColumnName="metric_name",
    valueColumnName="metric_value",
)

# COMMAND ----------

# ── Publish ──
# insertInto rather than saveAsTable: it writes into the existing table instead
# of replacing it, so the declared schema and the clustering survive. It
# resolves columns by position, so the select pins the order.
(
    daily_metrics
    .select(*KEY_COLUMNS, "metric_name", "metric_value")
    .write.mode("overwrite")
    .insertInto(table_name)
)
