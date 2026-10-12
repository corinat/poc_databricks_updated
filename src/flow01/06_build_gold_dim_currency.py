# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 01, step 6: Gold currency dimension.
#
# - Source: silver_currency, which is already checked.
# - This table is a projection: the four columns DIM_CURRENCY declares, in its
#   order, and nothing else. source_file and ingested_at stop here — they are
#   lineage for the Silver layer, not attributes of a currency.
# - No quality gate. The project's rules put DQX at the Bronze to Silver
#   boundary, so by the time a row reaches here it has already passed.
# - Write mode: full overwrite. The dimension is the whole of its source, so
#   there is nothing to merge.
#
# Every column of DIM_CURRENCY has a source, so unlike dim_product and
# dim_customer there is no NULL placeholder here.
#
# is_reporting_currency is carried rather than dropped: flow 01's API load
# reads it to decide which currencies to call, and the mart's PLN conversion is
# meaningful only against whichever row it marks.

# COMMAND ----------
# ── Parameters (passed by DABs job as job-level parameters) ──
# DABs dev mode prefixes schema names with dev_<username>_.
# Compute the correct defaults so interactive runs match the bundle.
_user = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]
_dev_silver_schema = f"dev_{_user}_silver"
_dev_gold_schema = f"dev_{_user}_gold"

dbutils.widgets.text("catalog", "poc_dev")
dbutils.widgets.text("silver_schema", _dev_silver_schema)
dbutils.widgets.text("gold_schema", _dev_gold_schema)

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")

silver_table = f"{catalog}.{silver_schema}.silver_currency"
table_name = f"{catalog}.{gold_schema}.dim_currency"

# Pinned column order, matching the DDL below and DIM_CURRENCY's own order.
DIM_COLUMNS = [
    "currency_code", "currency_name", "decimal_places", "is_reporting_currency",
]

# COMMAND ----------

# ── Declared schema ──
# DIM_CURRENCY from DATA_MODEL.md, in its order. Gold is what the marts and the
# dashboard read, so the shape is pinned in DDL.
# currency_code is the dimension's key and NOT NULL.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    currency_code         STRING NOT NULL,
    currency_name         STRING,
    decimal_places        INT,
    is_reporting_currency BOOLEAN
)
""")

# COMMAND ----------

# ── Publish ──
# insertInto rather than saveAsTable: it writes into the existing table instead
# of replacing it, so the declared schema survives. It resolves columns by
# position, so the select pins the order.
(
    spark.table(silver_table)
    .select(*DIM_COLUMNS)
    .write.mode("overwrite")
    .insertInto(table_name)
)
