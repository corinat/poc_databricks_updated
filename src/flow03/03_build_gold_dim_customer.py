# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 03, step 3: Gold customer dimension.
#
# - Source: silver_customer, which is already typed, deduped, hierarchy-walked
#   and checked.
# - This table is a projection: the five columns DIM_CUSTOMER declares, in its
#   order, and nothing else.
# - No quality gate. The project's rules put DQX at the Bronze to Silver
#   boundary, so by the time a row reaches here it has already passed.
# - Write mode: full overwrite. The dimension is the whole of its source, so
#   there is nothing to merge.
#
# The three hierarchy columns stop at Silver.
#
# level, hierarchy_path and root_customer_id are in silver_customer and in
# neither DIM_CUSTOMER nor the mart — the model declares them nowhere in Gold.
# Flow 06 asks that the hierarchy be flattened once, in Silver, and not redone
# in Gold; it does not ask Gold to carry it. parent_customer_id is here, so the
# relationship stays navigable by self-join, just not pre-flattened.
#
# Worth knowing rather than discovering later: the dashboard is told to read
# the Gold marts only, so a report by holding company would not have the
# hierarchy available. Nothing flow 07 asks for needs it.
#
# customer_type is declared and always NULL. The model has the column and
# neither customer file has a source for it. It is declared rather than omitted
# because Gold is the published contract and DATA_MODEL.md is a deliverable: a
# table with four columns against a model that declares five means the ER
# diagram and the table disagree. NULL makes the gap visible in the data.

# COMMAND ----------
from pyspark.sql import functions as F

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

silver_table = f"{catalog}.{silver_schema}.silver_customer"
table_name = f"{catalog}.{gold_schema}.dim_customer"

# COMMAND ----------

# ── Declared schema ──
# DIM_CUSTOMER from DATA_MODEL.md, in its order. Gold is what the marts and the
# dashboard read, so the shape is pinned in DDL: a query that stopped producing
# these columns fails the write instead of quietly changing the table.
# customer_id is the dimension's key and NOT NULL.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    customer_id        INT NOT NULL,
    customer_name      STRING,
    customer_type      STRING,
    country_code       STRING,
    parent_customer_id INT
)
""")

# COMMAND ----------

# ── Publish ──
# The columns are named rather than selected with *, so the hierarchy columns
# and the Silver tracking columns cannot leak into Gold. The cast on the NULL
# gives it the declared type instead of leaving it untyped.
(
    spark.table(silver_table)
    .select(
        "customer_id",
        "customer_name",
        F.lit(None).cast("string").alias("customer_type"),
        "country_code",
        "parent_customer_id",
    )
    .write.mode("overwrite")
    .insertInto(table_name)
)
