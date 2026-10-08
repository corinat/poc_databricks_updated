# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 03, step 1: Bronze table with the customer batches.
#
# - Source: every .csv file in the customer_data folder. Each file is a batch
#   of the same source, read in its own call so it is mapped by its own header
#   row. The batches use a different column order.
# - Only batches that are not in the table yet are loaded, so a run triggered
#   by one upload leaves the batches already ingested alone.
# - Column names are normalised to lower snake_case; values are untouched and
#   every column lands as STRING. Casting and renaming happen in Silver.
# - Write mode: append with mergeSchema. Each batch is a new delivery, and a
#   batch may bring a column the table does not have yet.
# - Trigger: file arrival on the customer_data folder of the volume.
#
# Each batch is loaded on its own, so one that fails does not hold up the
# others. The task then fails, naming every batch that did not load.
#
# Delta column names cannot contain spaces, so normalising the header is what
# makes a header like "customer id" writable.
#
# Headers are not validated. A column the table has not seen before is added
# by mergeSchema, and is mapped in Silver.

# COMMAND ----------
import re

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

source_dir = (
    f"/Volumes/{catalog}/{bronze_schema}/{volume_name}/data/customer_data"
)
table_name = f"{catalog}.{bronze_schema}.bronze_customer"

# COMMAND ----------


def normalise(name: str) -> str:
    """Header name to lower snake_case, with runs of whitespace as one _."""
    return re.sub(r"\s+", "_", name.strip().lower())


# COMMAND ----------

# ── Batches already in the table, so a run loads only new deliveries ──
# source_file holds the file name, which is what dbutils.fs.ls reports too.
# On the first run the table does not exist yet and nothing is skipped.
if spark.catalog.tableExists(table_name):
    already_loaded = {
        row.source_file
        for row in spark.table(table_name).select("source_file").distinct().collect()
    }
else:
    already_loaded = set()

# ── One batch per file, each with its own header ──
# The folder holds customer batches only, so every .csv in it is a batch.
new_batches = [
    f for f in dbutils.fs.ls(source_dir)
    if f.name.endswith(".csv") and f.name not in already_loaded
]

failures = []

for batch_file in sorted(new_batches, key=lambda f: f.name):
    try:
        # header=true takes the column names from the file. Without
        # inferSchema every column stays STRING, so mergeSchema only ever
        # adds columns.
        batch = (
            spark.read
            .option("header", "true")
            .csv(batch_file.path)
        )

        # Rename in place, then add the Bronze tracking columns.
        batch = (
            batch
            .toDF(*[normalise(c) for c in batch.columns])
            .withColumn("source_file", col("_metadata.file_name"))
            .withColumn("ingested_at", current_timestamp())
        )

        # saveAsTable resolves columns by name, so batches whose columns are
        # in a different order still line up.
        (
            batch.write
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(table_name)
        )
    except Exception as error:  # noqa: BLE001 - reported together below
        failures.append(f"{batch_file.name}: {error}")

# COMMAND ----------

# ── Fail the task, naming every batch that did not load ──
# A batch that fails is never written, so its name never reaches source_file
# and the next run picks it up again.
if failures:
    raise RuntimeError(
        "Batches not loaded:\n" + "\n".join(failures)
    )
