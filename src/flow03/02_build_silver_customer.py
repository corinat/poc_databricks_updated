# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Flow 03, step 2: Silver customer table with the hierarchy flattened.
#
# - Clean and rename to the data model: trim values, cast the ids to INT, and
#   reconcile the country columns into country_code.
# - Deduplicate: one row per customer_id. "Latest" is the most recent
#   ingested_at, so the newest batch to carry a customer wins.
# - Flatten the hierarchy with a recursive CTE, adding per customer:
#     level             0 for a root, +1 per generation down
#     hierarchy_path    the chain of names from the root to this customer
#     root_customer_id  the holding company at the top of its branch
#   A customer whose parent is not in the data starts a tree of its own, so no
#   row is lost. Those rows are the ones with level = 0 and a
#   parent_customer_id that is set.
# - Write mode: INSERT OVERWRITE into a table whose schema is declared here,
#   so the shape is pinned for Gold and the dashboard. One changed row shifts
#   the level and path of everything below it, so the table is rebuilt in full.

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

bronze_table = f"{catalog}.{bronze_schema}.bronze_customer"
table_name = f"{catalog}.{silver_schema}.silver_customer"

# COMMAND ----------

# ── Which column holds the country ──
# Bronze is written with mergeSchema, so its columns depend on which batches
# have arrived: one batch calls the column country, another country_code. Only
# the ones actually present can be referenced, so the expression is built from
# the Bronze schema rather than assuming both exist.
bronze_columns = spark.table(bronze_table).columns
country_columns = [c for c in ("country_code", "country") if c in bronze_columns]

if country_columns:
    country_expr = f"coalesce({', '.join(country_columns)})"
else:
    country_expr = "CAST(NULL AS STRING)"

# COMMAND ----------

# ── Declared schema ──
# Silver is consumed by Gold and the dashboard, so the shape is declared once
# here rather than derived from the query. A query that stopped producing
# these types fails the insert instead of silently changing the table.
# customer_id is NOT NULL: a batch whose id does not cast to an integer fails
# the task rather than landing an unusable row.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    customer_id        INT NOT NULL,
    customer_name      STRING,
    country_code       STRING,
    parent_customer_id INT,
    level              INT,
    hierarchy_path     STRING,
    root_customer_id   INT
)
""")

# COMMAND ----------

# ── Clean, dedupe, then walk the hierarchy ──
# INSERT OVERWRITE replaces every row: one changed row shifts the level and
# path of everything below it, so the table is rebuilt in full.
# The final SELECT lists the columns in the order declared above.
# A cycle in parent_customer_id would recurse forever; Spark's recursion level
# limit stops it and fails the task instead.
spark.sql(f"""
INSERT OVERWRITE {table_name}
WITH RECURSIVE cleaned AS (
    SELECT
        CAST(trim(customer_id) AS INT)                    AS customer_id,
        trim(customer_name)                               AS customer_name,
        upper(trim({country_expr}))                       AS country_code,
        CAST(nullif(trim(parent_customer_id), '') AS INT) AS parent_customer_id,
        source_file,
        ingested_at
    FROM {bronze_table}
),

-- One row per customer_id, newest batch first.
deduped AS (
    SELECT customer_id, customer_name, country_code, parent_customer_id
    FROM cleaned
    QUALIFY row_number() OVER (
        PARTITION BY customer_id
        ORDER BY ingested_at DESC, source_file
    ) = 1
),

-- Anchor: the customers that start a tree. Either they have no parent, or
-- their parent is not in the data, which would otherwise leave them and
-- everything under them unreachable by the recursion. Such a row is
-- identifiable afterwards as level = 0 with a parent_customer_id set.
-- NOT EXISTS, not NOT IN: a NULL customer_id anywhere in deduped would make
-- NOT IN evaluate to NULL for every row and the branch would never fire.
-- Then one generation per recursive step, carrying the root id down and
-- appending to the path.
hierarchy AS (
    SELECT
        customer_id,
        customer_name,
        country_code,
        parent_customer_id,
        0             AS level,
        customer_name AS hierarchy_path,
        customer_id   AS root_customer_id
    FROM deduped AS child
    WHERE child.parent_customer_id IS NULL
     OR NOT EXISTS (
           SELECT 1
           FROM deduped AS candidate_parent
           WHERE candidate_parent.customer_id = child.parent_customer_id
       )

    UNION ALL

    SELECT
        child.customer_id,
        child.customer_name,
        child.country_code,
        child.parent_customer_id,
        parent.level + 1,
        concat(parent.hierarchy_path, ' > ', child.customer_name),
        parent.root_customer_id
    FROM deduped AS child
    JOIN hierarchy AS parent
      ON child.parent_customer_id = parent.customer_id
)

SELECT
    customer_id,
    customer_name,
    country_code,
    parent_customer_id,
    level,
    hierarchy_path,
    root_customer_id
FROM hierarchy
""")
