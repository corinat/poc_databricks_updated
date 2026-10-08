# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 04, step 1: list the invoice PDFs that still have to be parsed.
#
# - Lists the PDFs in the invoices folder with dbutils.fs, no external package.
# - Drops the ones already in bronze_invoice, so only new invoices are parsed.
#   Parsing calls ai_parse_document and ai_extract once per file and takes
#   about 20 seconds, so not redoing finished work is what keeps a re-run
#   cheap rather than just tidy.
# - Publishes the remaining paths as a task value, which is the input to the
#   For Each task that fans out one parse run per file.

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

source_dir = f"/Volumes/{catalog}/{bronze_schema}/{volume_name}/data/invoices"
table_name = f"{catalog}.{bronze_schema}.bronze_invoice"

# COMMAND ----------

# ── Invoices already parsed ──
# On the first run the table does not exist yet and nothing is skipped.
if spark.catalog.tableExists(table_name):
    already_parsed = {
        row.source_file
        for row in spark.table(table_name).select("source_file").distinct().collect()
    }
else:
    already_parsed = set()

# COMMAND ----------

# ── The PDFs still to parse ──
# dbutils.fs.ls returns volume paths with a dbfs: prefix. It is stripped here
# so source_file holds the plain /Volumes path, which is the same value
# FACT_INVOICE keeps as pdf_path and the same one read_files is given.
pdf_paths = [
    f.path.removeprefix("dbfs:")
    for f in dbutils.fs.ls(source_dir)
    if f.name.lower().endswith(".pdf")
]

pending = sorted(path for path in pdf_paths if path not in already_parsed)

# COMMAND ----------

# ── Hand the list to the For Each task ──
# An empty list means every invoice is already parsed, and the For Each task
# then runs no iterations.
dbutils.jobs.taskValues.set(key="invoice_files", value=pending)
