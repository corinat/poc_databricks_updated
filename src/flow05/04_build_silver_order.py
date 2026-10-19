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
# - Write mode: merge/upsert on order_id for the rows that pass, deleting any
#   row the gate stops passing, so Silver holds exactly the current valid set;
#   append for the rows that do not pass.
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
# source_file and ingested_at are carried through too, and for a reason beyond
# lineage: they are the only record of which delivery won the dedupe. Once this
# table is written that decision is otherwise gone — Bronze holds every
# delivery of an order_id, so joining back cannot say which one was kept
# without re-running the dedupe.
#
# ingested_at here is when the row was DELIVERED, the value Bronze stamped, not
# when this notebook ran. "When was this Silver row last rebuilt" would be a
# different column, and conflating the two is the trap worth avoiding.
#
# Neither belongs in Gold. FACT_ORDER is the Gold shape, so fact_order should
# name its columns rather than SELECT *, and drop these two.
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
import sys

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from delta.tables import DeltaTable

# COMMAND ----------
# ── The shared quality-gate helpers ──
# src/common/dq.py is a plain module, not a notebook. Its directory is given by
# the common_path job parameter, resolved from workspace.file_path, and appended
# to sys.path so the import below resolves in every bundle target.
dbutils.widgets.text("common_path", "../common")
common_path = dbutils.widgets.get("common_path")
if common_path not in sys.path:
    sys.path.append(common_path)

from dq import (
    ensure_quarantine_table,
    require_references,
    require_rows_to_write,
    write_quarantine,
)
from transforms import cleaned_orders

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
# FACT_ORDER from DATA_MODEL.md, in its order, plus sales_channel and the two
# tracking columns carried from Bronze.
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
    sales_channel STRING,
    source_file   STRING,
    ingested_at   TIMESTAMP
)
""")

# COMMAND ----------

# ── The shared quarantine table ──
# Shared with every other Bronze to Silver gate, so its shape and the
# projection into it live in src/common/dq.py rather than here.
ensure_quarantine_table(spark, quarantine_table)

# COMMAND ----------

# ── Dedupe and cast ──
# The cleaning, with no write of its own: the result is the DataFrame the
# checks run against.
# The SQL lives in src/common/transforms.py so the dedupe can be called
# with a view over frames built in memory, which is how
# tests/test_transforms.py checks that the newest delivery wins and so that
# the replayed orders keep their sales_channel.
orders = cleaned_orders(spark, bronze_table)


# COMMAND ----------

# ── Apply the checks ──
# Each key matches a ref_df_name in checks/silver_order.yml.
dq_engine = DQEngine(WorkspaceClient())

ref_dfs = {
    "silver_customer": spark.table(f"{catalog}.{silver_schema}.silver_customer"),
    "bronze_product": spark.table(f"{catalog}.{bronze_schema}.bronze_product"),
    "bronze_currency": spark.table(f"{catalog}.{bronze_schema}.bronze_currency"),
}

# The currency foreign key is an error rather than a warning, and the write
# below can delete, so an empty bronze_currency would reject every order and
# empty silver_order from a run that looked successful. This fails instead,
# naming the table.
require_references(ref_dfs)

valid_orders, flagged_orders = dq_engine.apply_checks_by_metadata_and_split(
    orders, checks, ref_dfs=ref_dfs
)

# COMMAND ----------

# ── Nothing passed means nothing to write ──
# The merge below deletes the rows the source no longer carries, so an
# empty valid set would empty the table from a run that reports success.
# require_references above sees one way to get here, an empty reference
# table; this sees the rest.
require_rows_to_write(valid_orders, table_name)

# ── Upsert the rows that passed, and drop what stops passing ──
# The third clause is what makes the gate able to take a row back out. The
# first two only add and update: a row that was valid, reached Silver, and has
# since stopped passing — because a rule tightened, or its reference data
# changed, or it was removed from Bronze — is absent from valid_orders but
# would stay in the table for ever. whenNotMatchedBySourceDelete removes it,
# so Silver holds exactly what the gate currently passes.
#
# The consequence to know: Silver mirrors the gate, so an empty valid set
# empties the table. That is the same exposure every gate written with a full
# overwrite already has, and Delta time travel is the way back —
# RESTORE TABLE ... TO VERSION AS OF.
(
    DeltaTable.forName(spark, table_name).alias("target")
    .merge(valid_orders.alias("source"), "target.order_id = source.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()
    .execute()
)

# COMMAND ----------

# ── Record the rows that did not ──
# The payload columns are derived from the frame inside write_quarantine, so
# this call cannot fall out of step with silver_order's shape.
write_quarantine(flagged_orders, "bronze_order", quarantine_table)
