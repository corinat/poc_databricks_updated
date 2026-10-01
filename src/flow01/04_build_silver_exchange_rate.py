# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 01, step 4: Silver table combining historical and current exchange rates.
#
# - Combine bronze_exchange_rate_hist (CSV view) and bronze_exchange_rate_current (API).
# - Keep all columns from Bronze.
# - Deduplicate: one row per effectiveDate + code, latest ingested_at wins (QUALIFY).
# - Write mode: replace only the changed date(s), not the whole table
#   (INSERT INTO ... REPLACE USING, Databricks Runtime 17.2+ for unpartitioned tables).

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

hist_view = f"{catalog}.{bronze_schema}.bronze_exchange_rate_hist"
current_table = f"{catalog}.{bronze_schema}.bronze_exchange_rate_current"
table_name = f"{catalog}.{silver_schema}.silver_exchange_rate"

# Columns that come from the source files / API (tracking columns excluded).
source_columns_sql = "`table`, currency, code, no, effectiveDate, mid"
all_columns_sql = f"{source_columns_sql}, source_file, ingested_at"
# COMMAND ----------

# ── Combine historical and current data, deduplicate with QUALIFY ──
# One row per effectiveDate + code, latest ingested_at wins.
deduplicated_sql = f"""
SELECT {all_columns_sql}
FROM (
    SELECT {all_columns_sql} FROM {hist_view}
    UNION ALL
    SELECT {all_columns_sql} FROM {current_table}
) AS combined
QUALIFY row_number() OVER (PARTITION BY effectiveDate, code ORDER BY ingested_at DESC) = 1
"""

# COMMAND ----------

# ── First run: create the empty table with the Bronze columns ──
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name}
AS SELECT {all_columns_sql} FROM {hist_view} WHERE 1 = 0
""")

# ── Find changed dates: a date has a row that is new or differs from Silver ──
# Done as a separate query, because REPLACE USING cannot read from its own target.
changed_dates = [
    row.effectiveDate
    for row in spark.sql(f"""
        SELECT DISTINCT effectiveDate
        FROM (
            SELECT {source_columns_sql} FROM ({deduplicated_sql}) AS deduplicated
            EXCEPT
            SELECT {source_columns_sql} FROM {table_name}
        ) AS changed
    """).collect()
]

# ── Replace only the changed dates ──
# REPLACE USING deletes every Silver row whose effectiveDate matches an incoming row,
# then inserts the incoming rows.
if changed_dates:
    date_list = ", ".join(f"DATE'{d}'" for d in changed_dates)
    spark.sql(f"""
        INSERT INTO {table_name} BY NAME
        REPLACE USING (effectiveDate)
        SELECT {all_columns_sql}
        FROM ({deduplicated_sql}) AS deduplicated
        WHERE effectiveDate IN ({date_list})
    """)

