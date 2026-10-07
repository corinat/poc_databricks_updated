# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 02, job A: Bronze table with the product catalog from the Excel file.
#
# - Source: every .xlsx file in the products folder, read as-is. Column names
#   stay as the sheet holds them and every column lands as STRING; nothing is
#   cast. Typing and renaming to the data model happen downstream.
# - Write mode: append, through Auto Loader. A new file is a new delivery.
# - Trigger: file arrival on the products/ folder of the volume.
#
# Auto Loader with the native Excel reader (cloudFiles.format = "excel",
# Databricks Runtime 17.1+). No external package: flow step 5 asks to check
# first whether Databricks solves a step natively.
#
# Auto Loader is what makes "a new file is a new delivery" work. The file
# arrival trigger does not tell the job which file arrived, and its path
# cannot hold a wildcard. Auto Loader keeps a checkpoint of the files it has
# already processed, so each run picks up only new ones, however they are
# named, and several deliveries waiting at once are handled in one batch.
# pathGlobFilter is the only way to restrict this to .xlsx: the filtering the
# trigger cannot do happens here, on the reader.

# COMMAND ----------
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType, StructField, StructType

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

# The products folder is watched by the file arrival trigger, so it holds only
# product deliveries. The checkpoint sits outside data/, so writing to it
# never looks like a new arrival to the trigger.
source_dir = f"{volume_root}/data/products"
checkpoint_path = f"{volume_root}/_checkpoints/bronze_product_excel"

table_name = f"{catalog}.{bronze_schema}.bronze_product"

# COMMAND ----------

# ── Explicit schema — no schema inference ──
# Everything STRING: the file is landed as-is and nothing is cast here.
# unit_price becomes a decimal, and is renamed to the data model's list_price,
# further downstream.
product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", StringType(), True),
])

# COMMAND ----------

# ── Auto Loader stream over the products folder ──
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "excel")
    # Restrict to workbooks. The file arrival trigger cannot filter by
    # extension, so anything else dropped in the folder is ignored here.
    .option("pathGlobFilter", "*.xlsx")
    # The sheet has a single header row. More than one is not supported.
    .option("headerRows", 1)
    # Schema evolution is not supported for Excel with Auto Loader, and the
    # schema is given explicitly anyway.
    .option("cloudFiles.schemaEvolutionMode", "none")
    .schema(product_schema)
    .load(source_dir)
)

# ── Bronze metadata: source_file + ingested_at ──
df = (
    df
    .withColumn("source_file", col("_metadata.file_name"))
    .withColumn("ingested_at", current_timestamp())
)

# COMMAND ----------

# availableNow processes whatever is waiting and then stops, so the job task
# finishes instead of running as a continuous stream.
query = (
    df.writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(table_name)
)

query.awaitTermination()
