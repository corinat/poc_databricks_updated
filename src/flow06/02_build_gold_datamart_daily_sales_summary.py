# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 06, step 2: daily sales, aggregated from the joined mart.
#
# - Source: gold_datamart_order_sales_detail, and nothing else. DATA_MODEL
#   names it as this table's only source, which is why product_category is
#   carried on the detail mart rather than joined here.
# - Grain: one row per order_date + currency_code + product_category, as
#   DATA_MODEL declares and section 3's diagram groups.
# - Write mode: full overwrite. The summary is the whole of its source.
#
# Cancelled orders are excluded.
#
# net_sales is a revenue measure and a cancelled order is not revenue. In the
# current data six of the thirty orders are cancelled, 649.51 of 6240.03 PLN,
# so including them would report a tenth of the total as sales that did not
# happen. DAILY_SALES_SUMMARY has no status column and no separate measure, so
# there is nowhere for the distinction to live if the rows are counted in.
#
# The status is fact_order's, which is the current status the order files
# carry. order_status_changes.parquet is a separate feed with its own view of
# the same orders — the two disagree on ten of the twelve orders they share,
# and their cancelled sets do not overlap at all — but it has no amounts on it
# and so cannot define what is subtracted from a total. It is kept apart in
# silver_order_status_history, as flow 05 requires.
#
# What this costs: a date, currency and category whose every order was
# cancelled produces no row at all rather than a row of zeros. A day with no
# orders was already absent, so any query reading this table has to handle a
# missing row either way — including flow 07's alert on an order_count of zero.
#
# Two measures do not cover the same orders.
#
# net_sales and sales_pln come from fact_order through the detail mart, so they
# cover every order that passed the gate. gross_sales comes from the invoice,
# which an order may not have; sum skips the NULL, so gross_sales covers only
# invoiced orders. In the current data every order has exactly one invoice and
# the two agree, but the asymmetry is in the grain of the sources, not in this
# query. The same applies to sales_pln for an order whose currency has no
# published rate on or before its date.
#
# order_count is count(*), not a distinct count on order_id: the detail mart is
# one row per order by construction, which its own two window functions
# guarantee, so the two are equal and the cheaper one says what it means.

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

detail_table = f"{catalog}.{gold_schema}.gold_datamart_order_sales_detail"
table_name = f"{catalog}.{gold_schema}.gold_datamart_daily_sales_summary"

# COMMAND ----------

# ── Declared schema ──
# GOLD_DATAMART_DAILY_SALES_SUMMARY from DATA_MODEL.md, in its order. The three
# key columns are NOT NULL because they are the grain: a product whose id is
# missing from dim_product would group under a NULL category and fail this
# insert, which is the intended outcome — a dimension gap should stop the
# publish rather than create a nameless group the dashboard would show as a
# blank slice.
# Clustered on the two columns flow 07 parameterizes the dashboard by.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    sales_date       DATE NOT NULL,
    currency_code    STRING NOT NULL,
    product_category STRING NOT NULL,
    order_count      INT,
    item_quantity    INT,
    net_sales        DECIMAL(18,2),
    gross_sales      DECIMAL(18,2),
    sales_pln        DECIMAL(18,2)
)
CLUSTER BY (sales_date, currency_code)
""")

# COMMAND ----------

SUMMARY_COLUMNS = [
    "sales_date", "currency_code", "product_category",
    "order_count", "item_quantity", "net_sales", "gross_sales", "sales_pln",
]

# COMMAND ----------

# ── Aggregate ──
# The filter is on the status, not on the amounts: a cancelled order keeps its
# amounts in the detail mart, so excluding it has to be done by status here.
# Status values are lowercased in Silver, so the comparison needs no function.
#
# Every measure is cast to the type the table declares. The aggregates widen on
# their own — count and sum over an integer return BIGINT, and sum over
# DECIMAL(18,2) returns DECIMAL(28,2) — and writing a wider type into a
# narrower column is a narrowing assignment that the ANSI store assignment
# policy rejects. The casts are what make the insert match the DDL, not
# decoration.
summary = spark.sql(f"""
SELECT
    order_date            AS sales_date,
    currency_code,
    product_category,
    CAST(count(*)              AS INT)            AS order_count,
    CAST(sum(quantity)         AS INT)            AS item_quantity,
    CAST(sum(net_amount)       AS DECIMAL(18,2))  AS net_sales,
    CAST(sum(gross_amount)     AS DECIMAL(18,2))  AS gross_sales,
    CAST(sum(order_amount_pln) AS DECIMAL(18,2))  AS sales_pln
FROM {detail_table}
WHERE order_status <> 'cancelled'
GROUP BY order_date, currency_code, product_category
""")

# COMMAND ----------

# ── Publish ──
# insertInto rather than saveAsTable: it writes into the existing table instead
# of replacing it, so the declared schema and the clustering survive. It
# resolves columns by position, so the select pins the order.
(
    summary
    .select(*SUMMARY_COLUMNS)
    .write.mode("overwrite")
    .insertInto(table_name)
)
