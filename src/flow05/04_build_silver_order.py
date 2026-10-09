# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 05, step 5: Silver order table, one row per order_id.
#
# - Source: bronze_order, which holds every delivery that has landed. With all
#   three, that is 46 rows for 30 orders, because the deliveries replay each
#   other. The replays are resolved here.
# - Dedupe: one row per order_id, latest first by ingested_at then order_date.
# - Cast to the types DATA_MODEL.md gives FACT_ORDER, normalize status, and
#   rename to the data model: currency becomes currency_code and order_amount
#   becomes net_amount.
# - Quality gate: DQX, at the Bronze to Silver boundary, per flow 06 section 2.
#   Rules are in checks/silver_order.yml, not in this notebook.
# - Write mode: merge/upsert on order_id for the rows that pass; append for the
#   rows that do not.
#
# This table answers "what is this order now". What happened to it over time is
# silver_order_status_history, built by its own pipeline from the status change
# feed. The two are kept separate, and status here is the one bronze_order
# carries — the change feed is not read by this notebook.
#
# sales_channel is carried through, nullable. It is not a FACT_ORDER column,
# but flow 06 requires a column delivered later to land as nullable in Silver
# with older rows NULL, naming sales_channel and flow 05. Rows from the two
# deliveries whose files have no such field keep NULL.
#
# Which delivery wins a replayed order_id is decided by ingested_at, so it is
# whichever of the three landed last. The 8 orders that orders_schema_change
# .parquet replays therefore keep their sales_channel while that delivery is
# the most recent one to have carried them.
#
# How the DQX split works here.
#
# apply_checks_by_metadata_and_split returns two DataFrames. A row failing an
# error rule appears only in the second one. A row failing a warn rule appears
# in both: usable, but recorded. The first DataFrame carries no DQX result
# columns, which is why silver_order keeps exactly the shape declared below and
# needs nothing dropped.
#
# The foreign key rules name a reference DataFrame rather than a table, so
# checks/silver_order.yml holds no catalog or schema and is identical in every
# target. The names are bound to tables here, where the widgets are known.
#
# MERGE needs one row per key in its source, which the dedupe guarantees.
# Without it the statement fails rather than picking a row. It is the Delta
# Python API rather than SQL because the source is a DataFrame, and a temp view
# to make it addressable from SQL is what EXERCISE.md rules out.

# COMMAND ----------
import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from delta.tables import DeltaTable

# COMMAND ----------
# ── Parameters (passed by DABs job as job-level parameters) ──
# DABs dev mode prefixes schema names with dev_<username>_.
# Compute the correct defaults so interactive runs match the bundle.
_user = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]
_dev_bronze_schema = f"dev_{_user}_bronze_raw"
_dev_silver_schema = f"dev_{_user}_silver"

dbutils.widgets.text("catalog", "poc_dev")
dbutils.widgets.text("bronze_schema", _dev_bronze_schema)
dbutils.widgets.text("silver_schema", _dev_silver_schema)
dbutils.widgets.text("checks_path", "../../checks/silver_order.yml")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
checks_path = dbutils.widgets.get("checks_path")

bronze_table = f"{catalog}.{bronze_schema}.bronze_order"
table_name = f"{catalog}.{silver_schema}.silver_order"
quarantine_table = f"{catalog}.{silver_schema}.silver_dq_quarantine"

# COMMAND ----------

# ── The rules ──
# Read as plain YAML: apply_checks_by_metadata_and_split takes a list of dicts,
# and it validates them itself before applying.
with open(checks_path) as checks_file:
    checks = yaml.safe_load(checks_file)

# COMMAND ----------

# ── Declared schema ──
# Silver is consumed by Gold and the dashboard, so the shape is declared once
# here rather than derived from the query. The columns and their types are
# FACT_ORDER from DATA_MODEL.md, in its order, plus sales_channel.
# order_id is NOT NULL: it is the merge key, and a row without one could not be
# matched on a later run. The DQX rule of the same name is what keeps such a
# row out, so this constraint is the backstop rather than the gate.
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
    status        STRING,
    sales_channel STRING
)
""")

# COMMAND ----------

# ── The shared quarantine table ──
# One table for every source, as flow 06 section 2 asks. The sources have
# different columns, so the failing row is held as VARIANT rather than as a
# column per field — the same approach bronze_invoice already uses.
#
# failed_checks carries only the rule name and its message. DQX's own result
# struct has ten fields, most of them its bookkeeping, and pinning that shape
# in DDL would recouple this table to the library version.
#
# severity is derived from which result column was populated: error rows are
# here instead of in silver_order, warn rows are here as well as in it.
#
# CREATE TABLE IF NOT EXISTS, because every Bronze to Silver gate writes here
# and none of them owns the table.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {quarantine_table} (
    source_table   STRING NOT NULL,
    severity       STRING,
    row_data       VARIANT,
    failed_checks  ARRAY<STRUCT<name STRING, message STRING>>,
    quarantined_at TIMESTAMP
)
""")

# COMMAND ----------

# ── Dedupe and cast ──
# The cleaning, with no write of its own: the result is the DataFrame the
# checks run against.
orders = spark.sql(f"""
SELECT
    order_id,
    CAST(customer_id AS INT)            AS customer_id,
    product_id,
    currency                            AS currency_code,
    CAST(order_date AS DATE)            AS order_date,
    CAST(quantity AS INT)               AS quantity,
    CAST(unit_price AS DECIMAL(18,2))   AS unit_price,
    CAST(order_amount AS DECIMAL(18,2)) AS net_amount,
    lower(trim(status))                 AS status,
    sales_channel
FROM {bronze_table}
QUALIFY row_number() OVER (
    PARTITION BY order_id
    ORDER BY ingested_at DESC, order_date DESC
) = 1
""")

# COMMAND ----------

# ── Apply the checks ──
# Each key matches a ref_df_name in checks/silver_order.yml.
dq_engine = DQEngine(WorkspaceClient())

ref_dfs = {
    "silver_customer": spark.table(f"{catalog}.{silver_schema}.silver_customer"),
    "bronze_product": spark.table(f"{catalog}.{bronze_schema}.bronze_product"),
    "bronze_currency": spark.table(f"{catalog}.{bronze_schema}.bronze_currency"),
}

valid_orders, flagged_orders = dq_engine.apply_checks_by_metadata_and_split(
    orders, checks, ref_dfs=ref_dfs
)

# COMMAND ----------

# ── Upsert the rows that passed ──
(
    DeltaTable.forName(spark, table_name).alias("target")
    .merge(valid_orders.alias("source"), "target.order_id = source.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

# ── Record the rows that did not ──
# _errors and _warnings are NULL rather than empty when a row has none of that
# kind, which is why the two are combined by CASE rather than by concat.
(
    flagged_orders.selectExpr(
        "'bronze_order' AS source_table",
        "CASE WHEN _errors IS NOT NULL THEN 'error' ELSE 'warn' END AS severity",
        """parse_json(to_json(struct(
               order_id, customer_id, product_id, currency_code, order_date,
               quantity, unit_price, net_amount, status, sales_channel
           ))) AS row_data""",
        """transform(
               CASE
                   WHEN _errors IS NULL   THEN _warnings
                   WHEN _warnings IS NULL THEN _errors
                   ELSE concat(_errors, _warnings)
               END,
               e -> named_struct('name', e.name, 'message', e.message)
           ) AS failed_checks""",
        "current_timestamp() AS quarantined_at",
    )
    .write
    .mode("append")
    .saveAsTable(quarantine_table)
)
