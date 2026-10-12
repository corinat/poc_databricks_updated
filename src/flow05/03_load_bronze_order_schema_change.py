# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 05, step 3: Bronze landing for the delivery that adds a column.
#
# - Source: orders_schema_change.parquet, which carries the 9 columns the
#   other two deliveries have plus sales_channel.
# - Read as one batch, like step 1: a single known file, delivered once. And
#   like step 1 it is skipped when its name is already among the table's
#   source_file values, so re-running the job does not append it twice.
#   Unlike step 1 it has no parameter gate: this delivery is part of every run.
# - pathGlobFilter is load-bearing, and here it is also what makes the read
#   safe: reading the orders folder as a whole is what loses the new column.
#   The READ side note below says why.
# - Write mode: append, with schema evolution turned on explicitly through
#   mergeSchema on the write.
# - Trigger: none. Flow 05 does not specify one.
#
# The two mergeSchema options are different options, and flow 05 step 4 warns
# about both. Keeping them apart is the whole content of this step.
#
#   READ side   spark.read.option("mergeSchema", "true")
#               Reading many Parquet files in one call does not union their
#               schemas. Spark takes the schema from one file's footer, so a
#               column only some files carry can be absent with no error
#               raised — the silent disappearance step 4 describes. This load
#               sidesteps it entirely by reading one file, so it never has to
#               merge anything on the way in.
#
#   WRITE side  .option("mergeSchema", "true") on the Delta append
#               Delta's append validation is asymmetric. A column the table
#               has that the DataFrame does not is written as NULL; a column
#               the DataFrame has that the table does not is rejected. So this
#               delivery cannot land without the write-side option — and it
#               fails loudly rather than silently, which is the opposite of
#               the read-side risk.
#
#               This is a writer option, not a Spark conf, so the serverless
#               allowlist that rejects spark.databricks.delta.schema.autoMerge
#               .enabled does not apply to it. There is no job-level place to
#               put it: a pipeline takes Spark confs in its configuration
#               block, a serverless job environment takes only
#               environment_version and dependencies.
#
# Rows from the other two deliveries keep NULL for sales_channel. That is
# accurate rather than missing data: their files have no such field. Flow 06
# requires exactly this — a column delivered later lands nullable, with older
# rows NULL, and neither breaks the load nor drops the column.
#
# Order within the job. This task runs last of the three so the widening is a
# recorded event: bronze_order exists without sales_channel by the time this
# runs, and gains it here, which shows up as a schema change in its table
# history. Run this task first instead and the table has sales_channel from the
# outset, no widening is ever recorded, and the demonstration the step exists
# for is gone. The data ends up identical either way.
#
# Whichever of steps 1 and 2 created the table, this holds: when the historical
# load is skipped, step 2 creates bronze_order and this task still widens it.
#
# bronze_order goes from 11 columns to 12: each delivery's own, plus the two
# Bronze tracking columns.

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
source_file_name = "orders_schema_change.parquet"

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
# One file, so its own footer is the schema and sales_channel is certain to be
# in it. No read-side mergeSchema is needed, or would do anything.
orders = (
    spark.read
    .format("parquet")
    .option("pathGlobFilter", source_file_name)
    .load(source_dir)
)

# ── Bronze metadata: source_file + ingested_at ──
orders = (
    orders
    .withColumn("source_file", col("_metadata.file_name"))
    .withColumn("ingested_at", current_timestamp())
)

# COMMAND ----------

(
    orders.write
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(table_name)
)