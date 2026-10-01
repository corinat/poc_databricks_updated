# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import (BooleanType, IntegerType, StringType,
                               StructField, StructType)

# COMMAND ----------

# ── Explicit schema — no schema inference ──
currency_schema = StructType([
    StructField("currency_code", StringType(), True),
    StructField("currency_name", StringType(), True),
    StructField("decimal_places", IntegerType(), True),
    StructField("is_reporting_currency", BooleanType(), True),
])

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

source_path = f"/Volumes/{catalog}/{bronze_schema}/{volume_name}/data/currencies.csv"
table_name = f"{catalog}.{bronze_schema}.bronze_currency"

# COMMAND ----------

# ── Read CSV with explicit types ──
df = (
    spark.read
    .schema(currency_schema)
    .option("header", "true")
    .csv(source_path)
)

# ── Add Bronze metadata: source_file + ingested_at ──
df = (
    df
    .withColumn("source_file", col("_metadata.file_name"))
    .withColumn("ingested_at", current_timestamp())
)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable(table_name)
