# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 05, step 2: Bronze landing for the incremental order delivery.
#
# - Source: the orders_stream files in the orders folder, read with Auto
#   Loader rather than as a one-off batch, so a delivery added later is picked
#   up without this notebook changing.
# - Its own schema location and checkpoint, as step 2 asks for by name. Both
#   are keyed to this load alone: steps 1 and 3 are batch reads that keep no
#   checkpoint of their own, and the status change feed has its own pair.
# - pathGlobFilter is load-bearing: the orders folder also holds the other two
#   order deliveries and the status change feed, and the glob is what keeps
#   this load to its own files.
# - No schema is given. Parquet declares its own types, so they are read
#   rather than guessed, and recorded in the schema location.
# - Write mode: append.
# - Trigger: none. Flow 05 does not specify one.
#
# What the checkpoint does and does not fix.
#
# The checkpoint makes this step idempotent: re-running the task reads no file
# twice, so bronze_order does not grow. That is a property of Auto Loader
# rather than of append. Steps 1 and 3 reach the same place by a different
# route, comparing the file name against the table's source_file values, so all
# three deliveries are safe to re-run.
#
# What none of that fixes is the overlap between deliveries. These 8 orders are
# the last 8 of orders.parquet, delivered a second time — so when the historical
# load has run, bronze_order holds two rows for each of them. That is a property
# of the data and survives any checkpoint or guard; resolving it is step 5's
# dedupe on order_id.
#
# The rescued data column is dropped, not kept.
#
# Auto Loader supplies _rescued_data whenever it infers a schema. flow 02's
# product load and the status change feed both keep theirs, because both pin a
# schema and want drift visible rather than absorbed. This load is the other
# case: it infers, it accepts new columns, and it shares bronze_order with two
# batch writers that produce no such column. Keeping it would mean every run
# of this task asking Delta to widen the table, which the workspace refuses.
#
# The cost is real — a stream delivery whose values stop fitting their type
# loses them here, where flow 02 would have kept them. Nothing in the three
# order files does that, and flow 06 is where that check belongs.
#
# Why addNewColumns is stated rather than left to the default.
#
# The default is conditional: addNewColumns applies only while no schema is
# given, and adding a .schema() call later would silently turn it to none.
# These files never gain a column — orders_schema_change.parquet is step 3's
# file, not this one — so the mode is defensive. If a future stream delivery
# did bring a column, Auto Loader records the new schema and fails the task;
# the next run then succeeds with the wider schema.

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

# The schema location and checkpoint sit outside data/, so writing them is
# never mistaken for a file arriving in the orders folder.
schema_path = f"{volume_root}/_schemas/bronze_order_stream"
checkpoint_path = f"{volume_root}/_checkpoints/bronze_order_stream"

table_name = f"{catalog}.{bronze_schema}.bronze_order"

# COMMAND ----------

# ── Auto Loader stream over the stream deliveries ──
orders = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("pathGlobFilter", "orders_stream*.parquet")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(source_dir)
    # Auto Loader adds _rescued_data of its own accord whenever it infers a
    # schema, and it has to go. The two batch deliveries do not produce it, so
    # keeping it would make this stream the only writer bringing a column
    # bronze_order does not have — and a write that needs the table widened is
    # refused outright here, because automatic schema migration is not allowed
    # on a table with ACLs enabled. There is also nothing for it to rescue:
    # this load accepts a new column through addNewColumns instead, which is
    # the whole subject of step 3.
    # drop is a no-op when the column is absent, so this holds either way.
    .drop("_rescued_data")
)

# ── Bronze metadata: source_file + ingested_at ──
orders = (
    orders
    .withColumn("source_file", col("_metadata.file_name"))
    .withColumn("ingested_at", current_timestamp())
)

# COMMAND ----------

# availableNow processes whatever is waiting and then stops, so the job task
# finishes instead of running as a continuous stream.
query = (
    orders.writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(table_name)
)

query.awaitTermination()
