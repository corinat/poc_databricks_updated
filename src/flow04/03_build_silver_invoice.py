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
#   the rows are then checked before anything is written. The task fails naming
#   the PDFs and the fields at fault, and Silver keeps its previous contents
#   rather than publishing an invoice with holes in its money columns.
# - Write mode: INSERT OVERWRITE into a table whose schema is declared here.
#   Clustered by invoice_id rather than partitioned by it: invoice_id is
#   unique, so Hive partitioning would write one small file per invoice.
#   Clustering is table metadata and survives the overwrite, where an OPTIMIZE
#   ZORDER would have to be re-run after every rebuild.
#
# seller_id is not in the invoices. The PDFs carry the seller's name, address
# and tax id, so the id has to be resolved against dim_seller on tax_id, which
# is a business join and belongs in Gold.

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

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

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

# Fields an invoice is unusable without: the key, the date it is reported
# under, and the three money columns.
REQUIRED_COLUMNS = [
    "invoice_id", "invoice_date", "net_amount", "vat_amount", "gross_amount",
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
        CAST({extracted('order_id')}           AS STRING) AS order_id,
        try_cast({extracted('invoice_date')}   AS DATE)   AS invoice_date,
        CAST({extracted('invoice_status')}     AS STRING) AS invoice_status,
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
        PARTITION BY invoice_id
        ORDER BY ingested_at DESC, pdf_path
    ) = 1
)
"""

# COMMAND ----------

# ── Check every parsed row before writing anything ──
# Checked against flattened rather than deduped: rows whose invoice_id is
# missing would all fall in one NULL partition and the dedupe would keep only
# one of them, so a run would report one failed extraction out of several.
# pdf_path identifies the row whatever else is missing.
missing_flags = ", ".join(
    f"{column} IS NULL AS {column}_missing" for column in REQUIRED_COLUMNS
)
missing_filter = " OR ".join(f"{column} IS NULL" for column in REQUIRED_COLUMNS)

incomplete = spark.sql(f"""
{flattened_cte}
SELECT pdf_path, {missing_flags}
FROM flattened
WHERE {missing_filter}
ORDER BY pdf_path
""").collect()

if incomplete:
    report = "\n".join(
        "{}: {}".format(
            row.pdf_path,
            ", ".join(
                column for column in REQUIRED_COLUMNS
                if row[f"{column}_missing"]
            ),
        )
        for row in incomplete
    )
    raise RuntimeError(
        "Extraction incomplete, silver_invoice left unchanged. "
        "Fields not found, by file:\n" + report
    )

# COMMAND ----------

# ── Rewrite the table ──
# The select lists the columns in the order declared above.
spark.sql(f"""
INSERT OVERWRITE {table_name}
{flattened_cte}
SELECT {', '.join(SILVER_COLUMNS)}
FROM deduped
""")
