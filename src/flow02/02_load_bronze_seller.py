# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 02, job B: Bronze table with the seller master from the Delta files.
#
# - Source: sellers_delta/, read as-is with the Delta reader.
# - Write mode: append. Re-runs accumulate; duplicates are deduplicated
#   downstream, not here.
# - Trigger: nightly schedule. There is no file event to react to.
#
# Read as Delta, not as Parquet. The folder is a Delta table with a
# transaction log: version 1 overwrote version 0, so a tombstoned part file is
# still on disk. The Delta reader honours the log and returns the current
# version; a plain Parquet read would also pick up the removed file and
# duplicate rows.
#
# The table is managed. Unity Catalog does not allow an external table over
# this path: "You can't define a table on any data files or directories within
# a volume." An external table would need an external location over cloud
# storage, which the Free Edition workspace does not have.

# COMMAND ----------
from pyspark.sql.functions import current_timestamp, lit

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

source_dir_name = "sellers_delta"
source_path = (
    f"/Volumes/{catalog}/{bronze_schema}/{volume_name}/data/{source_dir_name}"
)

table_name = f"{catalog}.{bronze_schema}.bronze_seller"

# COMMAND ----------

# ── Read the Delta table as-is. Its own schema is the Bronze schema. ──
df = spark.read.format("delta").load(source_path)

# ── Bronze metadata: source_file + ingested_at ──
# source_file is the Delta folder, not a part file: the part names are an
# internal detail of the table and change on every overwrite.
df = (
    df
    .withColumn("source_file", lit(source_dir_name))
    .withColumn("ingested_at", current_timestamp())
)

# COMMAND ----------

df.write.mode("append").saveAsTable(table_name)
