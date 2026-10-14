# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 06, step 1: the joined mart, one row per order.
#
# - Source: the Gold star schema only. fact_order is the spine; the four
#   dimensions and the other two facts hang off it. Nothing here reads Silver
#   or Bronze.
# - Six left joins, as flow 06 section 3 specifies. Left, not inner: an order
#   with no invoice or no rate stays in the mart with NULLs rather than
#   disappearing from the sales figures.
# - Write mode: full overwrite. The mart is the whole of its sources.
# - No quality gate. DQX sits at the Bronze to Silver boundary, so every row
#   reaching here has already passed one.
#
# What each join contributes.
#
#   fact_invoice        invoice_id, seller_id, vat_amount, gross_amount
#   fact_exchange_rate  rate_to_pln, which order_amount_pln is derived from
#   dim_product         product_category
#   dim_currency        the reporting currency's implied rate
#   dim_customer        nothing
#   dim_seller          nothing
#
# The last two are here because section 3's join table lists them, and
# DATA_MODEL's column list for this mart gives them nothing to land in: every
# key they would resolve — customer_id, seller_id — is already carried by
# fact_order or by the invoice. No column is taken from either, so a
# customer_id missing from dim_customer leaves no trace in the output; the
# joins cannot validate anything by themselves.
#
# The validation section 4 asks for is a distinct comparison against the
# dimensions — "use distinct only to validate inputs ... not as a table of its
# own" — and it lives in the check task at the end of this job, where a
# mismatch can fail the run. What these two joins do contribute is indirect: a
# dimension with a duplicated key would multiply rows here and break the grain,
# which that same task catches.
#
# product_category is not in DATA_MODEL's column list for this mart, but
# section 3's diagram groups this table by category to build the daily summary,
# and DATA_MODEL names this table as that summary's only source. The column has
# to be here for both statements to hold.
#
# Keeping the grain at one row per order.
#
# Section 3 asks for the grain to be re-checked after the joins. Two of the six
# can multiply rows, and both are resolved before the join rather than checked
# after it:
#
#   fact_invoice is one row per invoice_id, and nothing stops two invoices from
#   carrying the same order_id. invoice_per_order keeps one — the latest
#   invoice_date, then the highest invoice_id to break a tie — so the join is
#   one-to-at-most-one by construction.
#
#   fact_exchange_rate has one row per date and currency, and the match is on a
#   range rather than an equality, so every published rate up to the order date
#   qualifies. rate_per_order keeps the latest of them.
#
# The four dimension joins depend on their keys being unique, which the
# is_unique DQX rule on each dimension's Silver table enforces upstream.
#
# Which rate an order is converted at.
#
# The most recent rate published on or before the order date. Flow 06 asks for
# the nearest date rather than an equal one; nearest in either direction would
# let an order be converted at a rate published after it happened, which is not
# how a transaction is valued. An order dated before the first published rate
# for its currency gets no rate, and so no order_amount_pln.
#
# In the current data every order date has a rate, so this resolves to an exact
# match throughout. It starts to matter when the API rows — which land under the
# date the job ran — sit alongside the historical ones, and on any future
# delivery with a gap.
#
# The reporting currency is the exception. NBP does not publish the zloty
# against itself, so a PLN order would find no rate at all and lose its PLN
# amount — the one currency needing no conversion. Its rate is 1 by definition,
# taken from dim_currency's is_reporting_currency flag rather than from the
# literal 'PLN', so the mart follows the reference data if the reporting
# currency is ever changed.

# COMMAND ----------
# ── Parameters (passed by DABs job as job-level parameters) ──
# DABs dev mode prefixes schema names with dev_<username>_.
# Compute the correct default so interactive runs match the bundle.
_user = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]
_dev_gold_schema = f"dev_{_user}_gold"

dbutils.widgets.text("catalog", "poc_dev")
dbutils.widgets.text("gold_schema", _dev_gold_schema)

catalog = dbutils.widgets.get("catalog")
gold_schema = dbutils.widgets.get("gold_schema")

fact_order = f"{catalog}.{gold_schema}.fact_order"
fact_invoice = f"{catalog}.{gold_schema}.fact_invoice"
fact_exchange_rate = f"{catalog}.{gold_schema}.fact_exchange_rate"
dim_customer = f"{catalog}.{gold_schema}.dim_customer"
dim_product = f"{catalog}.{gold_schema}.dim_product"
dim_currency = f"{catalog}.{gold_schema}.dim_currency"
dim_seller = f"{catalog}.{gold_schema}.dim_seller"

table_name = f"{catalog}.{gold_schema}.gold_datamart_order_sales_detail"

# COMMAND ----------

# ── Declared schema ──
# GOLD_DATAMART_ORDER_SALES_DETAIL from DATA_MODEL.md, in its order, with
# product_category added after the foreign keys. The dashboard and the two
# sink tables read this mart, so the shape is pinned in DDL and a query that
# stopped producing these types fails the insert.
# order_id is the grain and is NOT NULL. Clustered on the two columns flow 07
# parameterizes the dashboard by.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    order_id         STRING NOT NULL,
    order_date       DATE,
    customer_id      INT,
    product_id       STRING,
    seller_id        STRING,
    currency_code    STRING,
    product_category STRING,
    invoice_id       STRING,
    quantity         INT,
    net_amount       DECIMAL(18,2),
    vat_amount       DECIMAL(18,2),
    gross_amount     DECIMAL(18,2),
    rate_to_pln      DECIMAL(18,6),
    order_amount_pln DECIMAL(18,2),
    order_status     STRING
)
CLUSTER BY (order_date, currency_code)
""")

# COMMAND ----------

# Pinned column order, matching the DDL above. insertInto resolves by
# position, so the select below pins the order and the frame cannot drift
# from the table.
MART_COLUMNS = [
    "order_id", "order_date", "customer_id", "product_id", "seller_id",
    "currency_code", "product_category", "invoice_id", "quantity",
    "net_amount", "vat_amount", "gross_amount", "rate_to_pln",
    "order_amount_pln", "order_status",
]

# COMMAND ----------

# ── The joined mart ──
# Two CTEs reduce the many-row sources to one row per order before they are
# joined, which is what keeps the grain. The third holds the joined row so
# order_amount_pln can be derived from the resolved rate without repeating
# the coalesce.
detail = spark.sql(f"""
WITH invoice_per_order AS (
    SELECT order_id, invoice_id, seller_id, vat_amount, gross_amount
    FROM {fact_invoice}
    WHERE order_id IS NOT NULL
    QUALIFY row_number() OVER (
        PARTITION BY order_id
        ORDER BY invoice_date DESC, invoice_id DESC
    ) = 1
),

-- Every rate published on or before the order date qualifies; the latest of
-- them wins. An inner join here and a left join below, so an order with no
-- rate keeps its row and gets a NULL.
rate_per_order AS (
    SELECT o.order_id, r.rate_to_pln
    FROM {fact_order} AS o
    JOIN {fact_exchange_rate} AS r
      ON r.currency_code = o.currency_code
     AND r.rate_date <= o.order_date
    QUALIFY row_number() OVER (
        PARTITION BY o.order_id
        ORDER BY r.rate_date DESC
    ) = 1
),

joined AS (
    SELECT
        o.order_id,
        o.order_date,
        o.customer_id,
        o.product_id,
        i.seller_id,
        o.currency_code,
        p.category AS product_category,
        i.invoice_id,
        o.quantity,
        o.net_amount,
        i.vat_amount,
        i.gross_amount,
        -- The reporting currency converts at 1; everything else needs a
        -- published rate.
        coalesce(
            r.rate_to_pln,
            CASE WHEN c.is_reporting_currency THEN CAST(1 AS DECIMAL(18,6)) END
        ) AS rate_to_pln,
        o.status AS order_status
    FROM {fact_order} AS o
    LEFT JOIN {dim_customer}   AS cu ON o.customer_id   = cu.customer_id
    LEFT JOIN {dim_product}    AS p  ON o.product_id    = p.product_id
    LEFT JOIN {dim_currency}   AS c  ON o.currency_code = c.currency_code
    LEFT JOIN invoice_per_order AS i ON o.order_id      = i.order_id
    LEFT JOIN {dim_seller}     AS s  ON i.seller_id     = s.seller_id
    LEFT JOIN rate_per_order   AS r  ON o.order_id      = r.order_id
)

SELECT
    order_id,
    order_date,
    customer_id,
    product_id,
    seller_id,
    currency_code,
    product_category,
    invoice_id,
    quantity,
    net_amount,
    vat_amount,
    gross_amount,
    rate_to_pln,
    -- The delivered column flow 06 asks the rate join to produce. NULL when
    -- no rate applies, rather than silently falling back to the net amount.
    CAST(round(net_amount * rate_to_pln, 2) AS DECIMAL(18,2)) AS order_amount_pln,
    order_status
FROM joined
""")

# COMMAND ----------

# ── Publish ──
# insertInto rather than saveAsTable: it writes into the existing table instead
# of replacing it, so the declared schema and the clustering survive.
(
    detail
    .select(*MART_COLUMNS)
    .write.mode("overwrite")
    .insertInto(table_name)
)
