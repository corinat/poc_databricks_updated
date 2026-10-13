# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 01, step 7: Gold exchange rate fact.
#
# - Source: silver_exchange_rate, which is already deduped to one row per date
#   and currency, typed, and checked.
# - This is the one Gold build that renames. Silver keeps the NBP API's own
#   column names, because the quality requirement writes them that way —
#   "unique (effectiveDate, code), positive mid" — so the rename to the data
#   model happens here.
# - No quality gate. The project's rules put DQX at the Bronze to Silver
#   boundary, so by the time a row reaches here it has already passed.
# - Write mode: full overwrite. The fact is the whole of its source.
#
# The rename:
#
#   effectiveDate  ->  rate_date
#   code           ->  currency_code
#   mid            ->  rate_to_pln
#   table          ->  table_type       the NBP table type, 'A' for mid rates
#
# rate_source is carried through rather than worked out here.
#
# silver_exchange_rate sets it at the point where the two Bronze sources are
# combined, which is the one place the distinction cannot be mistaken. Deriving
# it here instead — by matching source_file against the API host — would
# reconstruct something already known, and would label every rate as coming
# from the CSV if this build and the API load ever disagreed about the host.
#
# It matters for a rate in a way it would not for a description: the API is the
# bank's own publication and the CSV is a copy, so an analyst looking at a
# discrepancy needs to know which one they have. It is also what survives of
# the dedupe's choice, which prefers the API when both carry the same date.
#
# Two columns stop at Silver. currency is the currency's name, which belongs to
# DIM_CURRENCY as currency_name rather than to a fact. no is the NBP table
# number, which the model does not declare.

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

silver_table = f"{catalog}.{silver_schema}.silver_exchange_rate"
table_name = f"{catalog}.{gold_schema}.fact_exchange_rate"


# COMMAND ----------

# ── Declared schema ──
# FACT_EXCHANGE_RATE from DATA_MODEL.md, in its order. Gold is what the marts
# and the dashboard read, so the shape is pinned in DDL.
# rate_date and currency_code are the fact's compound key and both NOT NULL.
# The DQX rules of the same names keep such a row out, so these are backstops.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    rate_date     DATE NOT NULL,
    currency_code STRING NOT NULL,
    rate_to_pln   DECIMAL(18,6),
    rate_source   STRING,
    table_type    STRING
)
""")

# COMMAND ----------

# ── Publish, renaming to the model ──
# `table` needs backticks: it is a reserved word in SQL.
# The columns are named rather than selected with *, so currency, no and the
# two tracking columns cannot leak into Gold.
spark.sql(f"""
INSERT OVERWRITE {table_name}
SELECT
    effectiveDate AS rate_date,
    code          AS currency_code,
    mid           AS rate_to_pln,
    rate_source,
    `table`       AS table_type
FROM {silver_table}
""")
