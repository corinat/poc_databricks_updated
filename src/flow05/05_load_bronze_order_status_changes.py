# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 05, step 6 input: Bronze landing for the order status change feed.
#
# - Source: the order_status_changes files in the orders folder, read with
#   Auto Loader, with their own schema location and checkpoint.
# - pathGlobFilter is load-bearing: the orders folder also holds the three
#   order deliveries, whose columns are entirely different from this feed's,
#   and the glob is what keeps this load to the status change files.
# - No schema is given. Parquet declares its own types, so they are read
#   rather than guessed, and recorded in the schema location.
# - schemaEvolutionMode is rescue, so that recorded schema stays fixed and
#   anything not fitting it is kept in _rescued_data instead of being dropped
#   or widening the table. Drift is then a query:
#   WHERE _rescued_data IS NOT NULL.
#   Holding the schema fixed also matters because the pipeline that builds
#   silver_order_status_history reads this table as a stream.
# - Write mode: append, and this table has to stay append-only for the same
#   reason: a stream cannot read a source whose rows are updated or deleted.
#
# This is a change feed, not a snapshot: several rows per order_id, each with
# a sequence_num, which is what the AUTO CDC flow downstream consumes.

# COMMAND ----------
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------
# ── Parameters (passed by DABs job as job-level parameters) ──
# Only the catalog, schema and volume are parameters: they are what changes
# between bundle targets. What this notebook loads is fixed below.
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

# The schema location and checkpoint sit outside data/, so writing them is
# never mistaken for a file arriving.
source_dir = f"{volume_root}/data/orders"
schema_path = f"{volume_root}/_schemas/bronze_order_status_changes"
checkpoint_path = f"{volume_root}/_checkpoints/bronze_order_status_changes"

table_name = f"{catalog}.{bronze_schema}.bronze_order_status_changes"

# COMMAND ----------

# ── Auto Loader stream over the status change files ──
changes = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("pathGlobFilter", "order_status_changes*.parquet")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("rescuedDataColumn", "_rescued_data")
    .load(source_dir)
)

# ── Bronze metadata: source_file + ingested_at ──
changes = (
    changes
    .withColumn("source_file", col("_metadata.file_name"))
    .withColumn("ingested_at", current_timestamp())
)

# COMMAND ----------

# availableNow processes whatever is waiting and then stops, so the job task
# finishes instead of running as a continuous stream.
query = (
    changes.writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(table_name)
)

query.awaitTermination()
