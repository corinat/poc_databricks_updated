# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 02, step 3: Silver product table, typed and renamed to the data model.
#
# - Source: bronze_product, where every column is STRING because the Excel is
#   landed as-is.
# - Cast and rename to DIM_PRODUCT: unit_price becomes list_price and becomes a
#   decimal. That one change is why this table exists — Silver is where this
#   project casts, and letting Gold do it would make Gold the only place that
#   does.
# - Dedupe: one row per product_id, newest ingested_at first, so a re-delivered
#   workbook replaces the catalogue rather than doubling it.
# - Quality gate: DQX, at the Bronze to Silver boundary. Rules are in
#   checks/silver_product.yml, not in this notebook.
# - Write mode: full overwrite. The catalogue is small and a delivery is the
#   whole of it, so there is nothing to merge.
#
# unit_of_measure is in DIM_PRODUCT and not here: the sheet has four columns and
# none of them is a unit, so there is no source for it.
#
# source_file and ingested_at are carried through. Beyond lineage they are the
# only record of which workbook won the dedupe — Bronze holds every delivery of
# a product_id, so joining back cannot say which one was kept. Neither belongs
# in Gold; dim_product should name its columns rather than SELECT *.

# COMMAND ----------
import sys

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

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
    require_rows_to_write,
    write_quarantine,
)

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
dbutils.widgets.text("checks_path", "../../checks/silver_product.yml")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
checks_path = dbutils.widgets.get("checks_path")

bronze_table = f"{catalog}.{bronze_schema}.bronze_product"
table_name = f"{catalog}.{silver_schema}.silver_product"

# Pinned column order, matching the DDL below, so the written frame cannot
# drift from the table. insertInto resolves by position.
SILVER_COLUMNS = [
    "product_id", "product_name", "category", "list_price",
    "source_file", "ingested_at",
]

# COMMAND ----------

# ── Declared schema ──
# Silver is consumed by Gold, so the shape is declared once here rather than
# derived from the query. A query that stopped producing these types fails the
# write instead of quietly changing the table.
# product_id is NOT NULL: it is the dimension's key. The DQX rule of the same
# name keeps such a row out, so this constraint is the backstop, not the gate.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    product_id   STRING NOT NULL,
    product_name STRING,
    category     STRING,
    list_price   DECIMAL(18,2),
    source_file  STRING,
    ingested_at  TIMESTAMP
)
""")

# COMMAND ----------

# ── Cast, rename and dedupe ──
# No write here: the result is the frame the checks run against.
products = spark.sql(f"""
SELECT
    product_id,
    product_name,
    category,
    CAST(unit_price AS DECIMAL(18,2)) AS list_price,
    source_file,
    ingested_at
FROM {bronze_table}
QUALIFY row_number() OVER (
    PARTITION BY product_id
    ORDER BY ingested_at DESC, source_file
) = 1
""")

# COMMAND ----------

# ── Apply the checks ──
# No ref_dfs: checks/silver_product.yml has no foreign key, because a product
# is a root dimension with nothing to point at.
with open(checks_path) as checks_file:
    checks = yaml.safe_load(checks_file)

dq_engine = DQEngine(WorkspaceClient())

valid_products, flagged_products = dq_engine.apply_checks_by_metadata_and_split(
    products, checks
)

# COMMAND ----------

# ── Nothing passed means nothing to write ──
# The write below replaces every row, so an empty valid set would empty
# the table from a run that reports success. An empty valid set is never
# worth writing: either there was nothing to load, or the gate rejected
# all of it.
require_rows_to_write(valid_products, table_name)

# COMMAND ----------

# ── Rewrite the table with the products that passed ──
# insertInto rather than saveAsTable: it writes into the existing table instead
# of replacing it, so the declared schema survives. It resolves columns by
# position, so the select pins the order.
(
    valid_products
    .select(*SILVER_COLUMNS)
    .write.mode("overwrite")
    .insertInto(table_name)
)

# COMMAND ----------

# ── Record the products that did not ──
# Shared with every other Bronze to Silver gate, so its shape and the
# projection into it live in src/common/dq.py rather than here.
quarantine_table = f"{catalog}.{silver_schema}.silver_dq_quarantine"

ensure_quarantine_table(spark, quarantine_table)
write_quarantine(flagged_products, "bronze_product", quarantine_table)
