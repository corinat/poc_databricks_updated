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
# - Two columns come from the file's text layer instead of the extraction, for
#   the reason below.
#
# Why two fields are read twice.
#
# The invoices print the order id and the status on one line:
#
#     Order ID: O-0001   Status: issued
#
# and then paint a white rectangle over it. In the content stream the text is
# written at y=466, and the next two operators set the fill colour to white and
# fill the rectangle from y=420 to y=476, which covers it; the table header is
# drawn on top. All 30 invoices are built the same way.
#
# So the line is in the file but not on the page. ai_parse_document reads the
# page as it renders — its options save rendered page images and its output
# carries bounding boxes — so ai_extract answers null for both of these fields,
# while invoice_id, one line higher and visible, comes back correctly. Nothing
# is recoverable from the VARIANT afterwards: the extraction answered "not
# found", and that answer is what is stored.
#
# This notebook already holds the bytes, so the second reading happens in the
# same query. One row then carries both readings of the document it describes,
# and Silver coalesces them with the extraction first.
#
# Why not drop ai_parse_document and extract from the text layer alone, which
# would see everything: the line items are one Tj operator per cell, with the
# column a value belongs to encoded only in its x coordinate and the header
# printed cell by cell. Reconstructing that table is what ai_parse_document
# does, and it is the part of this extraction that works best.
#
# Why a regex and not a second ai_extract over the same text: the labels sit
# directly beside the values, so there is nothing to infer, and a regex is
# deterministic and free where a second AI call per invoice is neither. It
# relies on the content stream being uncompressed, which is true of all 30 and
# is a property of this corpus rather than a general technique. A compressed
# stream yields nothing, the field stays NULL, and the order_id_is_null rule in
# the Silver gate quarantines the invoice instead of letting it through with a
# gap.

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
#
# decode with ISO-8859-1 rather than CAST AS STRING: it maps every byte to one
# character, where a UTF-8 cast would turn the binary parts of the file into
# replacement characters. The hidden line is ASCII either way.
#
# The two patterns anchor on the printed label rather than on the shape of the
# value, so an order id in another format is still found, and neither contains
# a backslash, so they do not depend on how a SQL string literal treats an
# escape. regexp_extract returns an empty string when nothing matches, which
# nullif turns into a NULL for the Silver gate to catch.
invoice = spark.sql(f"""
WITH doc AS (
    SELECT
        ai_parse_document(content, MAP('version', '{PARSE_VERSION}')) AS parsed,
        decode(content, 'ISO-8859-1')                                 AS content_stream
    FROM read_files('{pdf_path}', format => 'binaryFile')
)
SELECT
    parse_json(to_json(
        ai_extract(parsed, '{extraction_schema_sql}',
                MAP('version', '{EXTRACT_VERSION}'))
    ))                  AS parsed_data,
    nullif(
        regexp_extract(content_stream, 'Order ID:[ ]*([^ ]+)[ ]+Status:', 1),
        ''
    )                   AS order_id_text_layer,
    nullif(
        lower(regexp_extract(content_stream, 'Order ID:.*Status:[ ]*([A-Za-z]+)', 1)),
        ''
    )                   AS invoice_status_text_layer,
    '{pdf_path}'        AS source_file,
    current_timestamp() AS ingested_at
FROM doc
""")

# COMMAND ----------

invoice.write.mode("append").saveAsTable(table_name)
