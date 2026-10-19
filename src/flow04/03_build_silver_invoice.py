# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 04, step 3: Silver invoice header table, flattened out of the VARIANT.
#
# - One row per invoice. The items array stays in the Bronze VARIANT: this is
#   the header table, and the line items are nested by tax level and then by
#   item, which does not flatten into one row per invoice.
# - Columns are renamed to the data model: totals.total_net_amount becomes
#   net_amount, totals.currency becomes currency_code, and so on.
# - Deduplicate: one row per invoice_id, newest ingested_at first. The same
#   invoice can arrive twice as two differently named PDFs.
# - The extraction is an LLM call, so a field can come back missing. Dates and
#   amounts use try_cast, which yields NULL instead of failing the query, and
#   the rows are then put through DQX. An invoice missing any required field is
#   quarantined and never reaches Silver; the ones that are fine are published.
# - Quality gate: DQX, at the Bronze to Silver boundary. Rules are in
#   checks/silver_invoice.yml, not in this notebook.
#
# One bad extraction does not hold back the other 29: an error row goes to the
# quarantine with its reason recorded, and the invoices that passed are
# published.
#
# The consequence to know: silver_invoice can be incomplete. A total summed
# over it is then wrong without looking wrong. What makes the gap findable
# rather than silent is the quarantine row, which is what flow 07's quarantine
# alert watches.
# - Write mode: INSERT OVERWRITE into a table whose schema is declared here.
#   Clustered by invoice_id rather than partitioned by it: invoice_id is
#   unique, so Hive partitioning would write one small file per invoice.
#   Clustering is table metadata and survives the overwrite, where an OPTIMIZE
#   ZORDER would have to be re-run after every rebuild.
#
# seller_id is not in the invoices. The PDFs carry the seller's name, address
# and tax id, so the id has to be resolved against dim_seller on tax_id, which
# is a business join and belongs in Gold.
#
# Two readings of one document. order_id and invoice_status are printed on a
# line the invoices then cover with a white rectangle, so the page the
# extraction reads does not show them; step 2 reads the same bytes' text layer
# into order_id_text_layer and invoice_status_text_layer, and this notebook
# coalesces each pair with the extraction first. Every other column comes from
# the extraction alone.

# COMMAND ----------
import sys

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

# COMMAND ----------
# ── The shared quality-gate helpers ──
# src/common/dq.py is a plain module, not a notebook. Its directory is given by
# the common_path job parameter, resolved from workspace.file_path, and appended
# to sys.path so the import below resolves in every bundle target.
dbutils.widgets.text("common_path", "../common")
common_path = dbutils.widgets.get("common_path")
if common_path not in sys.path:
    sys.path.append(common_path)

from dq import (
    ensure_quarantine_table,
    require_rows_to_write,
    write_quarantine,
)

# COMMAND ----------
# ── Parameters (passed by DABs job as job-level parameters) ──
# DABs dev mode prefixes schema names with dev_<username>_.
# Compute the correct defaults so interactive runs match the bundle.
_user = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]
_dev_bronze_schema = f"dev_{_user}_bronze_raw"
_dev_silver_schema = f"dev_{_user}_silver"

dbutils.widgets.text("catalog", "poc_dev")
dbutils.widgets.text("bronze_schema", _dev_bronze_schema)
dbutils.widgets.text("silver_schema", _dev_silver_schema)
dbutils.widgets.text("checks_path", "../../checks/silver_invoice.yml")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
checks_path = dbutils.widgets.get("checks_path")

bronze_table = f"{catalog}.{bronze_schema}.bronze_invoice"
table_name = f"{catalog}.{silver_schema}.silver_invoice"

# Declared column order, reused by the insert.
SILVER_COLUMNS = [
    "invoice_id", "order_id", "invoice_date", "invoice_status", "place_of_issue",
    "buyer_name", "buyer_customer_id", "buyer_country",
    "seller_name", "seller_tax_id", "seller_street", "seller_postal_code",
    "seller_city",
    "currency_code", "net_amount", "vat_amount", "gross_amount",
    "pdf_path", "ingested_at",
]

# COMMAND ----------


def extracted(path: str) -> str:
    """VARIANT path to one extracted scalar. Every leaf is wrapped in value."""
    return f"parsed_data:response:{path}:value"


def amount(path: str) -> str:
    """An extracted amount as DECIMAL, whether it arrives as a number or as a
    locale-formatted string.

    The schema declares these as numbers, but a bare-label extraction returned
    them as text: '49,00' with a decimal comma, and '60,27 GBP' with the
    currency attached. The regexp drops anything that is not part of a number,
    then the comma becomes a decimal point. Both are no-ops on a clean number.
    try_cast so an unparseable amount becomes NULL and is reported below,
    rather than failing the query with no indication of which invoice it was.
    """
    return (
        "try_cast(replace(regexp_replace("
        f"CAST({extracted(path)} AS STRING), '[^0-9,.-]', ''), ',', '.')"
        " AS DECIMAL(18,2))"
    )


# COMMAND ----------

# ── Declared schema ──
# Silver is consumed by Gold and the dashboard, so the shape is declared once
# here rather than derived from the query.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    invoice_id         STRING NOT NULL,
    order_id           STRING,
    invoice_date       DATE,
    invoice_status     STRING,
    place_of_issue     STRING,
    buyer_name         STRING,
    buyer_customer_id  STRING,
    buyer_country      STRING,
    seller_name        STRING,
    seller_tax_id      STRING,
    seller_street      STRING,
    seller_postal_code STRING,
    seller_city        STRING,
    currency_code      STRING,
    net_amount         DECIMAL(18,2),
    vat_amount         DECIMAL(18,2),
    gross_amount       DECIMAL(18,2),
    pdf_path           STRING,
    ingested_at        TIMESTAMP
)
CLUSTER BY (invoice_id)
""")

# COMMAND ----------

# ── Flatten the VARIANT, one row per invoice ──
# Defined once and used by both the check and the insert below.
flattened_cte = f"""
WITH flattened AS (
    SELECT
        CAST({extracted('invoice_id')}         AS STRING) AS invoice_id,
        -- Extraction first, text layer second. The invoices paint a white
        -- rectangle over the line carrying these two fields, so the page the
        -- extraction reads does not show them; step 2 reads the same bytes'
        -- text layer into the two _text_layer columns. Extraction stays first
        -- so an invoice that does print the line visibly is read from the
        -- page, and this coalesce becomes a no-op without being removed.
        coalesce(
            CAST({extracted('order_id')} AS STRING), order_id_text_layer
        )                                                  AS order_id,
        try_cast({extracted('invoice_date')}   AS DATE)   AS invoice_date,
        coalesce(
            CAST({extracted('invoice_status')} AS STRING), invoice_status_text_layer
        )                                                  AS invoice_status,
        CAST({extracted('place_of_issue')}     AS STRING) AS place_of_issue,

        CAST({extracted('buyer:name')}         AS STRING) AS buyer_name,
        CAST({extracted('buyer:customer_id')}  AS STRING) AS buyer_customer_id,
        CAST({extracted('buyer:country')}      AS STRING) AS buyer_country,

        CAST({extracted('seller:name')}        AS STRING) AS seller_name,
        CAST({extracted('seller:tax_id')}      AS STRING) AS seller_tax_id,
        CAST({extracted('seller:street')}      AS STRING) AS seller_street,
        CAST({extracted('seller:postal_code')} AS STRING) AS seller_postal_code,
        CAST({extracted('seller:city')}        AS STRING) AS seller_city,

        upper(CAST({extracted('totals:currency')} AS STRING)) AS currency_code,
        {amount('totals:total_net_amount')}               AS net_amount,
        {amount('totals:total_tax_amount')}               AS vat_amount,
        {amount('totals:total_gross_amount')}             AS gross_amount,

        source_file AS pdf_path,
        ingested_at
    FROM {bronze_table}
),

deduped AS (
    SELECT {', '.join(SILVER_COLUMNS)}
    FROM flattened
    QUALIFY row_number() OVER (
        -- coalesce, not invoice_id alone: an extraction that lost the id
        -- leaves it NULL, and every such row would fall in one partition and
        -- all but one would be dropped here — so a run would quarantine one
        -- failed extraction out of several. pdf_path identifies the row
        -- whatever else is missing, so each survives to be reported.
        PARTITION BY coalesce(invoice_id, pdf_path)
        ORDER BY ingested_at DESC, pdf_path
    ) = 1
)
"""

# COMMAND ----------

# ── Apply the checks ──
# The key matches ref_df_name in checks/silver_invoice.yml.
with open(checks_path) as checks_file:
    checks = yaml.safe_load(checks_file)

invoices = spark.sql(f"""
{flattened_cte}
SELECT {', '.join(SILVER_COLUMNS)}
FROM deduped
""")

dq_engine = DQEngine(WorkspaceClient())

ref_dfs = {
    "silver_order": spark.table(f"{catalog}.{silver_schema}.silver_order"),
}

valid_invoices, flagged_invoices = dq_engine.apply_checks_by_metadata_and_split(
    invoices, checks, ref_dfs=ref_dfs
)

# COMMAND ----------

# ── Nothing passed means nothing to write ──
# The write below replaces every row, so an empty valid set would empty
# the table from a run that reports success. An empty valid set is never
# worth writing: either there was nothing to load, or the gate rejected
# all of it.
require_rows_to_write(valid_invoices, table_name)

# COMMAND ----------

# ── Rewrite the table with the invoices that passed ──
# insertInto rather than saveAsTable: it writes into the existing table instead
# of replacing it, so the declared schema and the clustering survive, which is
# what INSERT OVERWRITE did before. It resolves columns by position, so the
# select pins the order to the one declared above.
(
    valid_invoices
    .select(*SILVER_COLUMNS)
    .write.mode("overwrite")
    .insertInto(table_name)
)

# COMMAND ----------

# ── Record the invoices that did not ──
# Shared with every other Bronze to Silver gate, so its shape and the
# projection into it live in src/common/dq.py rather than here.
quarantine_table = f"{catalog}.{silver_schema}.silver_dq_quarantine"

ensure_quarantine_table(spark, quarantine_table)
write_quarantine(flagged_invoices, "bronze_invoice", quarantine_table)
