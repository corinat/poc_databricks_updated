# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 04, step 2: parse one invoice PDF into Bronze as VARIANT.
#
# Runs once per file, as the nested task of the For Each in step 1.
#
# - ai_parse_document turns the PDF into a structured document, and ai_extract
#   pulls the fields out of it. Both are native, so no PDF or LLM package is
#   installed.
# - The field definitions come from docs/invoice_extraction_schema.json, which
#   is already in ai_extract's advanced schema format: types, descriptions,
#   the invoice_status enum, and the nested buyer, seller, totals and items
#   structures. The descriptions carry the Polish labels the invoices print
#   (`Order ID`, `Razem do zaplaty`, `NIP`), which is what lets the fields be
#   found at all.
# - The whole ai_extract result is stored, error_message and metadata
#   included, so a partial extraction is visible in Bronze rather than
#   silently reduced to nulls.
# - VARIANT, not a struct: the items array is nested two levels deep, by tax
#   level and then by item, and Bronze keeps that shape without modelling it.
# - Write mode: append. One row per file.

# COMMAND ----------
import json

# COMMAND ----------
# ── Parameters ──
# pdf_path comes from the For Each task, one path per iteration.
# The rest are job-level parameters.
# DABs dev mode prefixes schema names with dev_<username>_.
_user = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]
_dev_bronze_schema = f"dev_{_user}_bronze_raw"

dbutils.widgets.text("catalog", "poc_dev")
dbutils.widgets.text("bronze_schema", _dev_bronze_schema)
dbutils.widgets.text("pdf_path", "")
# The job passes an absolute path built from ${workspace.file_path}, which
# differs per target. The default keeps an interactive run working from the
# notebook's own folder.
dbutils.widgets.text("schema_path", "../../docs/invoice_extraction_schema.json")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
pdf_path = dbutils.widgets.get("pdf_path")
schema_path = dbutils.widgets.get("schema_path")

table_name = f"{catalog}.{bronze_schema}.bronze_invoice"

# ai_parse_document and ai_extract are versioned; the versions are pinned so a
# new default cannot change what lands in Bronze.
PARSE_VERSION = "2.0"
EXTRACT_VERSION = "2.1"

# COMMAND ----------

# ── The extraction schema, read from the repo file ──
# Kept in the file rather than inlined here so the schema stays
# version-controlled and has one definition.
# Parsed and re-serialised rather than passed through as text: a malformed
# schema then fails here, with a parse error naming the position, instead of
# reaching ai_extract as an unusable argument. It also strips the formatting
# whitespace from the string embedded in the query below.
with open(schema_path) as schema_file:
    extraction_schema = json.load(schema_file)

# The schema goes into a single-quoted SQL string literal, so any single quote
# inside it has to be doubled.
extraction_schema_sql = json.dumps(extraction_schema).replace("'", "''")

# COMMAND ----------

# ── Parse the PDF, extract the fields, keep the result as VARIANT ──
# to_json then parse_json gives a VARIANT column whichever type ai_extract
# returns.
invoice = spark.sql(f"""
WITH doc AS (
    SELECT ai_parse_document(content, MAP('version', '{PARSE_VERSION}')) AS parsed
    FROM read_files('{pdf_path}', format => 'binaryFile')
)
SELECT
    parse_json(to_json(
        ai_extract(parsed, '{extraction_schema_sql}',
                MAP('version', '{EXTRACT_VERSION}'))
    ))                  AS parsed_data,
    '{pdf_path}'        AS source_file,
    current_timestamp() AS ingested_at
FROM doc
""")

# COMMAND ----------

invoice.write.mode("append").saveAsTable(table_name)
