# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 05, step 7: Gold order fact.
#
# - Source: silver_order, which is already deduped to one row per order_id,
#   cast to the model's types, and checked.
# - This table is a projection: the nine columns FACT_ORDER declares, in its
#   order, and nothing else.
# - No quality gate. The project's rules put DQX at the Bronze to Silver
#   boundary, so by the time a row reaches here it has already passed.
# - Write mode: full overwrite. The fact is the whole of its source, so there
#   is nothing to merge — and because silver_order now deletes rows the gate
#   stops passing, an overwrite here keeps the two in step.
#
# Three columns stop at Silver.
#
# sales_channel is the interesting one. It is in silver_order because flow 06
# requires a column delivered later to land nullable in Silver with older rows
# NULL, and it is not in FACT_ORDER, so Silver is where it ends. Anyone asking
# which channel an order came through queries Silver.
#
# source_file and ingested_at stop here too. They record which of the three
# deliveries won the dedupe, which is a question about how the row was built,
# not about the order.
#
# The grain is one row per order_id, inherited from silver_order's dedupe
# rather than established here. unit_price and net_amount are the measures;
# quantity is both a measure and, with order_date, what the daily summary
# aggregates over.

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

silver_table = f"{catalog}.{silver_schema}.silver_order"
table_name = f"{catalog}.{gold_schema}.fact_order"

# Pinned column order, matching the DDL below and FACT_ORDER's own order, so
# the written frame cannot drift from the table. insertInto resolves by
# position.
FACT_COLUMNS = [
    "order_id", "customer_id", "product_id", "currency_code", "order_date",
    "quantity", "unit_price", "net_amount", "status",
]

# COMMAND ----------

# ── Declared schema ──
# FACT_ORDER from DATA_MODEL.md, in its order. Gold is what the marts and the
# dashboard read, so the shape is pinned in DDL: a query that stopped producing
# these columns fails the write instead of quietly changing the table.
# order_id is the fact's key and NOT NULL. The three foreign keys are not
# constrained — flow 06 joins them with left joins precisely so a missing
# dimension row does not drop an order, and the quality gate already records a
# reference that is not there.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    order_id      STRING NOT NULL,
    customer_id   INT,
    product_id    STRING,
    currency_code STRING,
    order_date    DATE,
    quantity      INT,
    unit_price    DECIMAL(18,2),
    net_amount    DECIMAL(18,2),
    status        STRING
)
""")

# COMMAND ----------

# ── Publish ──
# The columns are named rather than selected with *, so sales_channel and the
# two tracking columns cannot leak into Gold.
(
    spark.table(silver_table)
    .select(*FACT_COLUMNS)
    .write.mode("overwrite")
    .insertInto(table_name)
)
