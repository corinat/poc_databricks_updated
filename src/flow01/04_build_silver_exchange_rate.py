# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 01, step 4: Silver table combining historical and current exchange
# rates, built with the DataFrame API. Not wired into a job.
#
# - Combine bronze_exchange_rate_hist (CSV view) and
#   bronze_exchange_rate_current (API).
# - Keep all columns from Bronze.
# - Deduplicate: one row per effectiveDate + code. The NBP API row wins over
#   the historical CSV row, because the API is the bank's own published
#   source. Within the same source, the latest ingested_at wins.
# - Write mode: replace every effectiveDate present in the incoming data,
#   through .option("replaceUsing", "effectiveDate"). Rows under other dates
#   are left untouched, and an empty source deletes nothing.
#
# Requires Databricks Runtime 18.2+ for the Python form of REPLACE USING.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

hist_view = f"{catalog}.{bronze_schema}.bronze_exchange_rate_hist"
current_table = f"{catalog}.{bronze_schema}.bronze_exchange_rate_current"
table_name = f"{catalog}.{silver_schema}.silver_exchange_rate"

# Pinned column order, so the written frame cannot drift from the table.
# "table" needs no backticks here: it is a column name, not parsed as SQL.
SILVER_COLUMNS = [
    "table", "currency", "code", "no", "effectiveDate", "mid",
    "source_file", "ingested_at",
]

# COMMAND ----------

# ── Combine both Bronze sources, tagging which one each row came from ──
# source_priority: 1 = API (wins), 2 = historical CSV. Used for ordering only
# and dropped before the write.
combined = (
    spark.table(current_table).withColumn("source_priority", F.lit(1))
    .unionByName(
        spark.table(hist_view).withColumn("source_priority", F.lit(2))
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

# ── First run: create the empty table with the Bronze columns ──
# replaceUsing writes into an existing table and does not create a missing
# one, so the table is created here first. limit(0) takes the schema without
# any rows, and mode("ignore") makes the step a no-op once the table exists.
(
    spark.table(hist_view)
    .select(*SILVER_COLUMNS)
    .limit(0)
    .write.mode("ignore")
    .saveAsTable(table_name)
)

# COMMAND ----------

# ── Write: replace every effectiveDate present in the incoming data ──
# replaceUsing deletes the Silver rows whose effectiveDate matches an incoming
# row, then inserts the incoming rows. Dates absent from the source keep their
# existing rows.
(
    deduplicated.write
    .mode("overwrite")
    .option("replaceUsing", "effectiveDate")
    .saveAsTable(table_name)
)
