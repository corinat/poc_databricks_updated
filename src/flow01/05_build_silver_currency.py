# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 01, step 5: Silver currency table.
#
# - Source: bronze_currency, whose columns are already the ones DIM_CURRENCY
#   wants, with the types it wants. Nothing is cast or renamed here.
# - Quality gate: DQX, at the Bronze to Silver boundary. Rules are in
#   checks/silver_currency.yml, not in this notebook.
# - Write mode: full overwrite. The reference list is the whole of its source,
#   so there is nothing to merge.
#
# Why this table exists at all, given it transforms nothing.
#
# dim_currency has to come from somewhere, and the only alternative is Bronze.
# That would make currency the one entity whose Gold table reads Bronze
# directly, skipping the layer where this project casts, conforms and checks —
# and it would leave the approved-currency list as the only reference data in
# the project that nothing validates, while five other tables join on it.
#
# No dedupe, unlike the other Silver builds.
#
# bronze_currency is written with a full overwrite, so a currency cannot arrive
# twice across runs and there is nothing for a dedupe to resolve. Leaving it
# out is what lets the uniqueness rule do real work: it catches two rows for
# one code in the source file, which a dedupe would silently have resolved by
# picking one.
#
# source_file and ingested_at are carried through, as in the other Silver
# tables. Neither belongs in Gold; dim_currency names its columns instead.

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

from dq import ensure_quarantine_table, write_quarantine

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
dbutils.widgets.text("checks_path", "../../checks/silver_currency.yml")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
checks_path = dbutils.widgets.get("checks_path")

bronze_table = f"{catalog}.{bronze_schema}.bronze_currency"
table_name = f"{catalog}.{silver_schema}.silver_currency"

# Pinned column order, matching the DDL below, so the written frame cannot
# drift from the table. insertInto resolves by position.
SILVER_COLUMNS = [
    "currency_code", "currency_name", "decimal_places", "is_reporting_currency",
    "source_file", "ingested_at",
]

# COMMAND ----------

# ── Declared schema ──
# The DIM_CURRENCY columns, in its order, plus the two tracking columns.
# currency_code is NOT NULL: it is the key five other tables join on. The DQX
# rule of the same name keeps such a row out, so this is the backstop.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    currency_code         STRING NOT NULL,
    currency_name         STRING,
    decimal_places        INT,
    is_reporting_currency BOOLEAN,
    source_file           STRING,
    ingested_at           TIMESTAMP
)
""")

# COMMAND ----------

# ── Apply the checks ──
# No ref_dfs: checks/silver_currency.yml has no foreign key, because a currency
# is a root dimension. Five tables point at it; it points at nothing.
with open(checks_path) as checks_file:
    checks = yaml.safe_load(checks_file)

dq_engine = DQEngine(WorkspaceClient())

valid_currencies, flagged_currencies = dq_engine.apply_checks_by_metadata_and_split(
    spark.table(bronze_table).select(*SILVER_COLUMNS), checks
)

# COMMAND ----------

# ── Rewrite the table with the currencies that passed ──
# insertInto rather than saveAsTable: it writes into the existing table instead
# of replacing it, so the declared schema survives. It resolves columns by
# position, so the select pins the order.
(
    valid_currencies
    .select(*SILVER_COLUMNS)
    .write.mode("overwrite")
    .insertInto(table_name)
)

# COMMAND ----------

# ── Record the currencies that did not ──
# Shared with every other Bronze to Silver gate, so its shape and the
# projection into it live in src/common/dq.py rather than here.
quarantine_table = f"{catalog}.{silver_schema}.silver_dq_quarantine"

ensure_quarantine_table(spark, quarantine_table)
write_quarantine(flagged_currencies, "bronze_currency", quarantine_table)
