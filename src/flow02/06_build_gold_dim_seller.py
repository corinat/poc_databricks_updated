# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 02, step 6: Gold seller dimension.
#
# - Source: silver_seller, which is already typed, deduped and checked.
# - This table is a projection: the eight columns DIM_SELLER declares, in its
#   order, and nothing else. source_file and ingested_at stop here — they are
#   lineage for the Silver layer, not attributes of a seller.
# - No quality gate. The project's rules put DQX at the Bronze to Silver
#   boundary, so by the time a row reaches here it has already passed.
# - Write mode: full overwrite. The dimension is the whole of its source, so
#   there is nothing to merge.
#
# Nothing is cast or renamed. silver_seller's columns are already the ones the
# model asks for, with the types it asks for, which is why this step is short.
#
# The columns are named rather than selected with *, so the two tracking
# columns cannot leak into Gold when Silver gains another one.

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

silver_table = f"{catalog}.{silver_schema}.silver_seller"
table_name = f"{catalog}.{gold_schema}.dim_seller"

# Pinned column order, matching the DDL below and DIM_SELLER's own order, so
# the written frame cannot drift from the table. insertInto resolves by
# position.
DIM_COLUMNS = [
    "seller_id", "seller_name", "tax_id",
    "address", "postal_code", "city", "country", "currency_code",
]

# COMMAND ----------

# ── Declared schema ──
# DIM_SELLER from DATA_MODEL.md, in its order. Gold is what the marts and the
# dashboard read, so the shape is pinned in DDL: a query that stopped producing
# these columns fails the write instead of quietly changing the table.
# seller_id is the dimension's key and NOT NULL.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    seller_id     STRING NOT NULL,
    seller_name   STRING,
    tax_id        STRING,
    address       STRING,
    postal_code   STRING,
    city          STRING,
    country       STRING,
    currency_code STRING
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
