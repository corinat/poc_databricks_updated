# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 01, step 4: Silver table combining historical and current exchange
# rates, built with the DataFrame API.
#
# - Combine bronze_exchange_rate_hist (CSV view) and
#   bronze_exchange_rate_current (API).
# - Keep all columns from Bronze, and add rate_source: which of the two
#   sources the surviving row came from. It is set here because this is the one
#   place the distinction is unambiguous.
# - Deduplicate: one row per effectiveDate + code. The NBP API row wins over
#   the historical CSV row, because the API is the bank's own published
#   source. Within the same source, the latest ingested_at wins.
# - Quality gate: DQX, at the Bronze to Silver boundary. Rules are in
#   checks/silver_exchange_rate.yml, not in this notebook.
# - Write mode: replace every effectiveDate present in the incoming data,
#   through .option("replaceUsing", "effectiveDate"). Rows under other dates
#   are left untouched, and an empty source deletes nothing.
#
# One consequence of replaceUsing together with the gate: a date is replaced
# whenever the incoming data has any row for it. So if one rate for a date is
# quarantined and another passes, the date is rewritten from the passing rows
# alone, and the quarantined pair disappears from Silver rather than keeping an
# earlier value. If every row for a date is quarantined, the date is absent
# from the incoming frame and its existing rows are left alone.
#
# Column names are the NBP API's own. Renaming to FACT_EXCHANGE_RATE happens
# downstream, which is also how the quality requirement writes them —
# "unique (effectiveDate, code), positive mid".
#
# Requires Databricks Runtime 18.2+ for the Python form of REPLACE USING.

# COMMAND ----------
import sys

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
dbutils.widgets.text("checks_path", "../../checks/silver_exchange_rate.yml")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
checks_path = dbutils.widgets.get("checks_path")

hist_view = f"{catalog}.{bronze_schema}.bronze_exchange_rate_hist"
current_table = f"{catalog}.{bronze_schema}.bronze_exchange_rate_current"
table_name = f"{catalog}.{silver_schema}.silver_exchange_rate"

# Pinned column order, so the written frame cannot drift from the table.
# "table" needs no backticks here: it is a column name, not parsed as SQL.
SILVER_COLUMNS = [
    "table", "currency", "code", "no", "effectiveDate", "mid",
    "rate_source", "source_file", "ingested_at",
]

# COMMAND ----------

# ── Combine both Bronze sources, tagging which one each row came from ──
# Two tags, from the one point where the distinction cannot be mistaken:
# source_priority orders the dedupe and is dropped, rate_source is kept.
#
# rate_source is written here rather than worked out downstream. Gold declares
# the column, and the alternative — matching source_file against the API host
# in the Gold build — would reconstruct a fact that is already known here, and
# would silently label every rate as coming from the CSV if the two ever
# disagreed about the host. There is nothing to disagree about this way.
combined = (
    spark.table(current_table)
    .withColumn("source_priority", F.lit(1))
    .withColumn("rate_source", F.lit("NBP_API"))
    .unionByName(
        spark.table(hist_view)
        .withColumn("source_priority", F.lit(2))
        .withColumn("rate_source", F.lit("HISTORICAL_CSV"))
    )
)

# ── Deduplicate: one row per effectiveDate + code ──
# Number the rows in each group, keep the first, then drop the counter.
dedup_order = Window.partitionBy("effectiveDate", "code").orderBy(
    F.col("source_priority").asc(),
    F.col("ingested_at").desc(),
)

deduplicated = (
    combined
    .withColumn("_rn", F.row_number().over(dedup_order))
    .filter(F.col("_rn") == 1)
    .select(*SILVER_COLUMNS)
)

# COMMAND ----------

# ── Declared schema ──
# replaceUsing writes into an existing table and does not create a missing one,
# so the table has to exist before the write. It is declared rather than taken
# from hist_view with limit(0): Silver is consumed by Gold, so the shape is
# pinned here, and a query that stopped producing these types fails the write
# instead of quietly changing the table.
# `table` needs backticks — it is a reserved word in SQL, unlike in the Python
# column list above.
# effectiveDate and code are NOT NULL: they are the pair the write replaces on
# and the pair uniqueness is defined over. The DQX rules of the same name keep
# such a row out, so these constraints are the backstop rather than the gate.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    `table`       STRING,
    currency      STRING,
    code          STRING NOT NULL,
    no            STRING,
    effectiveDate DATE NOT NULL,
    mid           DECIMAL(18,6),
    rate_source   STRING,
    source_file   STRING,
    ingested_at   TIMESTAMP
)
""")

# COMMAND ----------

# ── Apply the checks ──
# The key matches ref_df_name in checks/silver_exchange_rate.yml.
with open(checks_path) as checks_file:
    checks = yaml.safe_load(checks_file)

dq_engine = DQEngine(WorkspaceClient())

ref_dfs = {
    "bronze_currency": spark.table(f"{catalog}.{bronze_schema}.bronze_currency"),
}

valid_rates, flagged_rates = dq_engine.apply_checks_by_metadata_and_split(
    deduplicated, checks, ref_dfs=ref_dfs
)

# COMMAND ----------

# ── Write the rows that passed ──
# replaceUsing deletes the Silver rows whose effectiveDate matches an incoming
# row, then inserts the incoming rows. Dates absent from the source keep their
# existing rows.
(
    valid_rates.write
    .mode("overwrite")
    .option("replaceUsing", "effectiveDate")
    .saveAsTable(table_name)
)

# COMMAND ----------

# ── Record the rows that did not ──
# Shared with every other Bronze to Silver gate, so its shape and the
# projection into it live in src/common/dq.py rather than here. The payload
# columns are derived from the frame, so this call cannot fall out of step with
# the table's shape — which matters here, where one column is called `table`.
quarantine_table = f"{catalog}.{silver_schema}.silver_dq_quarantine"

ensure_quarantine_table(spark, quarantine_table)
write_quarantine(flagged_rates, "bronze_exchange_rate", quarantine_table)
