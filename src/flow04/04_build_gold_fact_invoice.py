# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 04, step 4: Gold invoice fact.
#
# - Source: silver_invoice, which is already deduped to one row per invoice_id,
#   cast, and checked.
# - Nine of FACT_INVOICE's ten columns come straight from Silver. The tenth,
#   seller_id, is not in the invoices at all and is resolved here.
# - No quality gate. The project's rules put DQX at the Bronze to Silver
#   boundary, so by the time a row reaches here it has already passed.
# - Write mode: full overwrite. The fact is the whole of its source.
#
# Why this step has real work, unlike the other Gold builds.
#
# The PDFs print the seller's name, address and tax id, but never a seller_id —
# that identifier exists only in the seller master. So the fact's seller_id has
# to be looked up, and a lookup against another entity's dimension is a
# business join, which is why DATA_MODEL.md's own note puts it in Gold rather
# than in Silver.
#
# The join is on tax_id, because it is the only field the two sides share that
# identifies a legal entity. Names would be worse: the PDF prints a trading
# name that an extraction can render slightly differently, where a tax id is a
# registration number.
#
# LEFT JOIN, deliberately. Flow 06 asks for left joins wherever a missing
# reference must not drop a row, and an invoice whose seller cannot be resolved
# is still a real invoice for real money. It lands with seller_id NULL, which
# is visible, rather than disappearing.
#
# Two things to know about the join rather than guard against:
#
#   Format. Both sides currently hold the tax id as 111-111-11-11, so a direct
#   comparison matches. If an extraction ever renders it differently — without
#   dashes, say — the join misses and seller_id comes out NULL. That is visible
#   in the data, and normalising both sides by stripping non-digits is the fix
#   if it happens. It is not done pre-emptively because it would be inventing a
#   matching rule for a case that does not exist yet.
#
#   Cardinality. tax_id is not a declared key of dim_seller, so two sellers
#   sharing one would fan the join out and break the one-row-per-invoice grain.
#   That is why checks/silver_seller.yml carries an error-level uniqueness rule
#   on tax_id: duplicates never reach the dimension, so the join cannot fan
#   out. Flow 06 asks that cardinality be confirmed per join, and
#   count(*) against count(DISTINCT invoice_id) is how.
#
# Ten columns of silver_invoice stop at Silver: the buyer's name, id and
# country, the seller's name and address, the seller's own tax id, the place of
# issue, and ingested_at. FACT_INVOICE declares none of them.

# COMMAND ----------
# ── Parameters (passed by DABs job as job-level parameters) ──
# DABs dev mode prefixes schema names with dev_<username>_.
# Compute the correct defaults so interactive runs match the bundle.
_user = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]
_dev_silver_schema = f"dev_{_user}_silver"
_dev_gold_schema = f"dev_{_user}_gold"

dbutils.widgets.text("catalog", "poc_dev")
dbutils.widgets.text("silver_schema", _dev_silver_schema)
dbutils.widgets.text("gold_schema", _dev_gold_schema)

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")

silver_table = f"{catalog}.{silver_schema}.silver_invoice"
seller_table = f"{catalog}.{gold_schema}.dim_seller"
table_name = f"{catalog}.{gold_schema}.fact_invoice"

# COMMAND ----------

# ── Declared schema ──
# FACT_INVOICE from DATA_MODEL.md, in its order. Gold is what the marts and the
# dashboard read, so the shape is pinned in DDL.
# invoice_id is the fact's key and NOT NULL. The three foreign keys are not
# constrained: seller_id is NULL when the lookup misses, and flow 06 joins the
# others with left joins so a missing dimension row does not drop an invoice.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    invoice_id     STRING NOT NULL,
    order_id       STRING,
    seller_id      STRING,
    currency_code  STRING,
    invoice_date   DATE,
    net_amount     DECIMAL(18,2),
    vat_amount     DECIMAL(18,2),
    gross_amount   DECIMAL(18,2),
    pdf_path       STRING,
    invoice_status STRING
)
""")

# COMMAND ----------

# ── Publish, resolving seller_id ──
# The columns are named rather than selected with *, so the buyer and seller
# detail that FACT_INVOICE does not declare cannot leak into Gold.
spark.sql(f"""
INSERT OVERWRITE {table_name}
SELECT
    i.invoice_id,
    i.order_id,
    s.seller_id,
    i.currency_code,
    i.invoice_date,
    i.net_amount,
    i.vat_amount,
    i.gross_amount,
    i.pdf_path,
    i.invoice_status
FROM {silver_table} AS i
LEFT JOIN {seller_table} AS s
       ON i.seller_tax_id = s.tax_id
""")
