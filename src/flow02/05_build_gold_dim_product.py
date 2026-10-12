# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 02, step 5: Gold product dimension.
#
# - Source: silver_product, which is already typed, deduped and checked.
# - This table is a projection: the five columns DIM_PRODUCT declares, in its
#   order, and nothing else. source_file and ingested_at stop here — they are
#   lineage for the Silver layer, not attributes of a product.
# - No quality gate. The project's rules put DQX at the Bronze to Silver
#   boundary, so by the time a row reaches here it has already passed.
# - Write mode: full overwrite. The dimension is the whole of its source, so
#   there is nothing to merge.
#
# unit_of_measure is declared and always NULL.
#
# The sheet has four columns and none of them is a unit. It cannot be derived
# either: unit_price says what a unit costs, not what the unit is, and 49 is
# the same number whether it is per piece or per kilogram. Every product in the
# current catalogue happens to be countable merchandise, and orders carry
# integer quantities, so "piece" would be a fair guess — but a guess written
# into a dimension is invented reference data, so it is left unset.
#
# It is declared rather than omitted because Gold is the published contract and
# DATA_MODEL.md is a deliverable: a table with four columns against a model
# that declares five means the ER diagram and the table disagree. NULL makes
# the gap visible in the data instead, and a catalogue that later carries the
# unit needs no DDL change.

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

silver_table = f"{catalog}.{silver_schema}.silver_product"
table_name = f"{catalog}.{gold_schema}.dim_product"

# COMMAND ----------

# ── Declared schema ──
# DIM_PRODUCT from DATA_MODEL.md, in its order. Gold is what the marts and the
# dashboard read, so the shape is pinned in DDL: a query that stopped producing
# these columns fails the write instead of quietly changing the table.
# product_id is the dimension's key and NOT NULL.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    product_id      STRING NOT NULL,
    product_name    STRING,
    category        STRING,
    list_price      DECIMAL(18,2),
    unit_of_measure STRING
)
""")

# COMMAND ----------

# ── Publish ──
# The columns are named rather than selected with *, so a column added to
# Silver cannot leak into Gold. The cast on the NULL gives it the declared type
# instead of leaving it untyped.
spark.sql(f"""
INSERT OVERWRITE {table_name}
SELECT
    product_id,
    product_name,
    category,
    list_price,
    CAST(NULL AS STRING) AS unit_of_measure
FROM {silver_table}
""")
