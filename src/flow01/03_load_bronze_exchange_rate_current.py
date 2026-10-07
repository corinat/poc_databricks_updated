# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 01, step 3: Bronze table with the current exchange rates from the NBP API.
#
# - Currency list comes from bronze_currency: every currency marked
#   is_reporting_currency = false (so PLN is left out today). NBP publishes
#   foreign rates against PLN, so there is no endpoint for it.
# - Same columns and types as the view bronze_exchange_rate_hist (step 2).
# - Write mode: full overwrite. The table only holds the latest snapshot.
#
# Databricks native http_request() SQL function + a Unity Catalog
# HTTP connection (flow step 5: "check first if PySpark/Databricks already solves
# a step natively before adding an external package").
# Previous version (Python requests library) is kept below, commented out.

# COMMAND ----------
# ── Parameters (passed by DABs job as job-level parameters) ──
# DABs dev mode prefixes schema names with dev_<username>_.
# Compute the correct default so interactive runs match the bundle.
_user = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]
_dev_bronze_schema = f"dev_{_user}_bronze_raw"

dbutils.widgets.text("catalog", "poc_dev")
dbutils.widgets.text("bronze_schema", _dev_bronze_schema)

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")

currency_table = f"{catalog}.{bronze_schema}.bronze_currency"
table_name = f"{catalog}.{bronze_schema}.bronze_exchange_rate_current"

# Unity Catalog HTTP connection to the NBP API (created in the next cell).
connection_name = "nbp_api"

# Base URL, used to fill source_file.
NBP_RATES_URL = "https://api.nbp.pl/api/exchangerates/rates/A"

# COMMAND ----------

# ── HTTP connection to the NBP API (created once, skipped if it exists) ──
# The NBP API needs no authentication. The connection still requires an auth
# option, so a placeholder bearer token is set; NBP ignores it.
spark.sql(f"""
CREATE CONNECTION IF NOT EXISTS {connection_name}
TYPE HTTP
OPTIONS (
    host 'https://api.nbp.pl',
    port '443',
    base_path '/api/exchangerates',
    bearer_token 'not-used'
)
""")

# COMMAND ----------

# ── Call the NBP API once per foreign currency and parse the JSON ──
# The reporting currency is excluded through the is_reporting_currency flag
# rather than a hardcoded 'PLN'. Only currencies explicitly marked false are
# called; the flag is not defaulted when it is missing.
# Same columns and types as bronze_exchange_rate_hist.
# A non-200 response stops the task, like raise_for_status() in the previous version.
df = spark.sql(f"""
WITH currencies AS (
    SELECT currency_code
    FROM {currency_table}
    WHERE is_reporting_currency = false
),
responses AS (
    SELECT
        currency_code,
        http_request(
            conn    => '{connection_name}',
            method  => 'GET',
            path    => concat('/rates/A/', currency_code, '/'),
            params  => map('format', 'json')
        ) AS response
    FROM currencies
),
parsed AS (
    SELECT
        currency_code,
        CASE
            WHEN response.status_code <> 200 THEN
                raise_error(concat('NBP API returned ', response.status_code,
                                   ' for ', currency_code, ': ', response.text))
        END AS _status_check,
        from_json(
            response.text,
            '`table` STRING, currency STRING, code STRING,
             rates ARRAY<STRUCT<no: STRING, effectiveDate: DATE, mid: DECIMAL(18,6)>>'
        ) AS payload
    FROM responses
)
SELECT
    payload.`table`                                                 AS `table`,
    payload.currency                                                AS currency,
    payload.code                                                    AS code,
    rate.no                                                         AS no,
    rate.effectiveDate                                              AS effectiveDate,
    rate.mid                                                        AS mid,
    concat('{NBP_RATES_URL}/', currency_code, '/?format=json')      AS source_file,
    current_timestamp()                                             AS ingested_at
FROM parsed
LATERAL VIEW explode(payload.rates) AS rate
WHERE _status_check IS NULL
""")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable(table_name)
