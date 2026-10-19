# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 02, step 4: Silver seller table.
#
# - Source: bronze_seller, whose columns are already the ones DIM_SELLER wants,
#   all STRING, and the model wants them all STRING. Nothing is cast or
#   renamed here.
# - Dedupe: one row per seller_id, newest ingested_at first. This is the whole
#   reason the table exists. bronze_seller is appended in full on every load,
#   so the same seller is present once per run, and source_file is the folder
#   name rather than a file name so it cannot tell the deliveries apart.
# - Quality gate: DQX, at the Bronze to Silver boundary. Rules are in
#   checks/silver_seller.yml, not in this notebook.
# - Write mode: full overwrite. The master is small and a delivery is the whole
#   of it, so there is nothing to merge.
#
# source_file and ingested_at are carried through, as in the other Silver
# tables. Neither belongs in Gold; dim_seller should name its columns rather
# than SELECT *.

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
dbutils.widgets.text("checks_path", "../../checks/silver_seller.yml")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
checks_path = dbutils.widgets.get("checks_path")

bronze_table = f"{catalog}.{bronze_schema}.bronze_seller"
table_name = f"{catalog}.{silver_schema}.silver_seller"

# Pinned column order, matching the DDL below, so the written frame cannot
# drift from the table. insertInto resolves by position.
SILVER_COLUMNS = [
    "seller_id", "seller_name", "tax_id",
    "address", "postal_code", "city", "country", "currency_code",
    "source_file", "ingested_at",
]

# COMMAND ----------

# ── Declared schema ──
# The DIM_SELLER columns, in its order. Silver is consumed by Gold, so the
# shape is declared here rather than derived from the query.
# seller_id is NOT NULL: it is the dimension's key. The DQX rule of the same
# name keeps such a row out, so this constraint is the backstop, not the gate.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    seller_id     STRING NOT NULL,
    seller_name   STRING,
    tax_id        STRING,
    address       STRING,
    postal_code   STRING,
    city          STRING,
    country       STRING,
    currency_code STRING,
    source_file   STRING,
    ingested_at   TIMESTAMP
)
""")

# COMMAND ----------

# ── Dedupe ──
# No write here: the result is the frame the checks run against.
# source_file is the same value on every row, so ingested_at alone decides and
# the tiebreak is the key itself, which keeps the result deterministic when two
# loads land in the same second.
sellers = spark.sql(f"""
SELECT
    seller_id,
    seller_name,
    tax_id,
    address,
    postal_code,
    city,
    country,
    currency_code,
    source_file,
    ingested_at
FROM {bronze_table}
QUALIFY row_number() OVER (
    PARTITION BY seller_id
    ORDER BY ingested_at DESC, seller_id
) = 1
""")

# COMMAND ----------

# ── Apply the checks ──
# The key matches ref_df_name in checks/silver_seller.yml.
with open(checks_path) as checks_file:
    checks = yaml.safe_load(checks_file)

dq_engine = DQEngine(WorkspaceClient())

ref_dfs = {
    "bronze_currency": spark.table(f"{catalog}.{bronze_schema}.bronze_currency"),
}

valid_sellers, flagged_sellers = dq_engine.apply_checks_by_metadata_and_split(
    sellers, checks, ref_dfs=ref_dfs
)

# COMMAND ----------

# ── Nothing passed means nothing to write ──
# The write below replaces every row, so an empty valid set would empty
# the table from a run that reports success. An empty valid set is never
# worth writing: either there was nothing to load, or the gate rejected
# all of it.
require_rows_to_write(valid_sellers, table_name)

# COMMAND ----------

# ── Rewrite the table with the sellers that passed ──
# insertInto rather than saveAsTable: it writes into the existing table instead
# of replacing it, so the declared schema survives. It resolves columns by
# position, so the select pins the order.
(
    valid_sellers
    .select(*SILVER_COLUMNS)
    .write.mode("overwrite")
    .insertInto(table_name)
)

# COMMAND ----------

# ── Record the sellers that did not ──
# Shared with every other Bronze to Silver gate, so its shape and the
# projection into it live in src/common/dq.py rather than here.
quarantine_table = f"{catalog}.{silver_schema}.silver_dq_quarantine"

ensure_quarantine_table(spark, quarantine_table)
write_quarantine(flagged_sellers, "bronze_seller", quarantine_table)
