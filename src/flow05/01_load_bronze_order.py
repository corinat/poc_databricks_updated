# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 05, step 1: Bronze landing for the historical order snapshot.
#
# - Source: orders.parquet, the full 30-order snapshot.
# - Read as one batch, not through Auto Loader. This is the initial historical
#   load of a single known file, so there is no arrival of new files to track
#   and nothing for a checkpoint to remember.
# - pathGlobFilter is load-bearing: the orders folder also holds the stream
#   delivery, the schema change delivery and the status change feed, and the
#   glob is what keeps this load to its own file.
# - No schema is given. Parquet declares its own types, so they are read
#   rather than guessed. Casting and renaming to the data model happen in
#   Silver.
# - Write mode: append.
# - Trigger: none. Flow 05 does not specify one, and these are fixed files
#   rather than a folder that keeps receiving deliveries.
#
# This task runs only when asked for.
#
# orders.parquet is a backfill, not an ongoing delivery, so the job gates this
# task behind its load_historical parameter: a condition task runs it only on
# the "true" outcome, and a routine run skips it altogether. The other two
# deliveries have no such gate and run every time.
#
# Append on its own is not idempotent, so this file is loaded once.
#
# The write mode is append, as the flow asks. Append alone would put the same
# 30 orders into bronze_order again on every run, so the load is guarded the
# way flow 03's customer load is guarded: the file is skipped when its name is
# already among the table's source_file values. Re-running the job is then a
# no-op here instead of a second copy.
#
# This does not make bronze_order one row per order. The three deliveries
# genuinely repeat rows — orders_stream.parquet replays the last 8 orders and
# orders_schema_change.parquet replays the first 8, both already in
# orders.parquet — and resolving that is Silver's job, by dedupe and merge on
# order_id in step 5.
#
# Why no mergeSchema here, when step 3 needs it.
#
# Delta's append validation is asymmetric. A column the table has that this
# DataFrame does not is written as NULL; a column this DataFrame has that the
# table does not raises an error unless mergeSchema is set. This file carries
# 9 columns and never adds one, so once the step 3 delivery has widened the
# table with sales_channel, this append still succeeds and leaves
# sales_channel NULL on its rows — which is accurate: orders.parquet has no
# such field.

# COMMAND ----------
from pyspark.sql.functions import col, current_timestamp

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

volume_root = f"/Volumes/{catalog}/{bronze_schema}/{volume_name}"
source_dir = f"{volume_root}/data/orders"

table_name = f"{catalog}.{bronze_schema}.bronze_order"

# The one file this step owns. Used both to restrict the read and to decide
# whether it has been loaded already, so the two can never drift apart.
source_file_name = "orders.parquet"

# COMMAND ----------

# ── Skip the file if it is already in the table ──
# source_file holds the file name, so the table itself records which deliveries
# have landed. Comparing against it needs no checkpoint and makes a re-run of
# the job a no-op here rather than a second copy of the same rows.
# On the first run the table does not exist and nothing is skipped.
if spark.catalog.tableExists(table_name):
    already_loaded = {
        row.source_file
        for row in spark.table(table_name).select("source_file").distinct().collect()
    }
else:
    already_loaded = set()

if source_file_name in already_loaded:
    dbutils.notebook.exit(f"{source_file_name} is already in {table_name}")

# COMMAND ----------

# ── Batch read of the one file this step owns ──
orders = (
    spark.read
    .format("parquet")
    .option("pathGlobFilter", source_file_name)
    .load(source_dir)
)

# ── Bronze metadata: source_file + ingested_at ──
# source_file is what makes a delivery identifiable after the fact, which is
# how Silver's dedupe decides which replay of an order_id to keep.
orders = (
    orders
    .withColumn("source_file", col("_metadata.file_name"))
    .withColumn("ingested_at", current_timestamp())
)

# COMMAND ----------

# saveAsTable resolves columns by name, so the three deliveries line up
# whatever order their columns are in.
(
    orders.write
    .mode("append")
    .saveAsTable(table_name)
)
