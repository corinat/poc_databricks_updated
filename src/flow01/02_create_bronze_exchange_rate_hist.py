# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 01, step 2: Bronze view over the historical exchange-rate CSV.
#
# - A VIEW, not a managed table (required by the flow).
# - Explicit types, no schema inference.
# - Column names stay as in the source (NBP API format); renaming happens in Silver.
# - ingested_at = file modification time, because a view has no load moment of its own.

# COMMAND ----------

# ── Parameters (passed by DABs job as job-level parameters) ──
# DABs dev mode prefixes schema names with dev_<username>_.
# Compute the correct default so interactive runs match the bundle.
_user = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]
_dev_bronze_schema = f"dev_{_user}_bronze_raw"

dbutils.widgets.text("catalog", "poc_dev")
dbutils.widgets.text("bronze_schema", _dev_bronze_schema)
dbutils.widgets.text("volume_name", "input_data")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
volume_name = dbutils.widgets.get("volume_name")

source_path = f"/Volumes/{catalog}/{bronze_schema}/{volume_name}/data/exchange_rates_historical.csv"
view_name = f"`{catalog}`.`{bronze_schema}`.bronze_exchange_rate_hist"

# ── Explicit schema, shared shape with bronze_exchange_rate_current (step 3) ──
# `table` is a reserved word, so it is quoted.
# mid is DECIMAL, not DOUBLE: rates feed money calculations.
exchange_rate_schema = (
    "`table` STRING, "
    "currency STRING, "
    "code STRING, "
    "no STRING, "
    "effectiveDate DATE, "
    "mid DECIMAL(18,6)"
)

# COMMAND ----------

# ── Create or replace the view (idempotent, safe to re-run) ──
spark.sql(f"""
CREATE OR REPLACE VIEW {view_name}
AS
SELECT
    `table`,
    currency,
    code,
    no,
    effectiveDate,
    mid,
    _metadata.file_name              AS source_file,
    _metadata.file_modification_time AS ingested_at
FROM read_files(
    '{source_path}',
    format     => 'csv',
    header     => true,
    schema     => '{exchange_rate_schema}'
)
""")

