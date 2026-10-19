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
# - Quality gate: DQX, at the Bronze to Silver boundary. Rules are in
#   checks/silver_customer.yml, not in this notebook. It runs on the finished
#   hierarchy, because the recursion has to happen in SQL and checking earlier
#   would need a temp view, which EXERCISE.md rules out.
# - source_file and ingested_at are carried through. Beyond lineage, they are
#   the only record of which batch won the dedupe — Bronze holds every delivery
#   of a customer_id, so joining back cannot say which one was kept.
# - Write mode: INSERT OVERWRITE into a table whose schema is declared here,
#   so the shape is pinned for Gold and the dashboard. One changed row shifts
#   the level and path of everything below it, so the table is rebuilt in full.

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
from transforms import customer_hierarchy

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
dbutils.widgets.text("checks_path", "../../checks/silver_customer.yml")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
checks_path = dbutils.widgets.get("checks_path")

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
    root_customer_id   INT,
    source_file        STRING,
    ingested_at        TIMESTAMP
)
""")

# COMMAND ----------

# Pinned column order, matching the DDL above, so the written frame cannot
# drift from the table. insertInto resolves by position.
SILVER_COLUMNS = [
    "customer_id", "customer_name", "country_code", "parent_customer_id",
    "level", "hierarchy_path", "root_customer_id",
    "source_file", "ingested_at",
]

# COMMAND ----------

# ── Clean, dedupe, then walk the hierarchy ──
# No write here: the result is the frame the checks run against.
# The SQL lives in src/common/transforms.py so it can be called with a view
# over frames built in memory, which is how tests/test_transforms.py exercises
# the recursion, the orphan anchor and the dedupe. Nothing about what runs here
# changes: the function receives this notebook's table name.
customers = customer_hierarchy(spark, bronze_table, country_expr)

# COMMAND ----------

# ── Apply the checks ──
# The reference frame is this same frame: the orphan rule asks whether a
# parent_customer_id is among the customer_ids present, which is a question
# about the table itself.
with open(checks_path) as checks_file:
    checks = yaml.safe_load(checks_file)

dq_engine = DQEngine(WorkspaceClient())

valid_customers, flagged_customers = dq_engine.apply_checks_by_metadata_and_split(
    customers, checks, ref_dfs={"silver_customer": customers}
)

# COMMAND ----------

# ── Nothing passed means nothing to write ──
# The write below replaces every row, so an empty valid set would empty
# the table from a run that reports success. An empty valid set is never
# worth writing: either there was nothing to load, or the gate rejected
# all of it.
require_rows_to_write(valid_customers, table_name)

# COMMAND ----------

# ── Rewrite the table with the customers that passed ──
# INSERT OVERWRITE replaces every row: one changed row shifts the level and
# path of everything below it, so the table is rebuilt in full.
# insertInto rather than saveAsTable: it writes into the existing table instead
# of replacing it, so the declared schema survives. It resolves columns by
# position, so the select pins the order.
(
    valid_customers
    .select(*SILVER_COLUMNS)
    .write.mode("overwrite")
    .insertInto(table_name)
)

# COMMAND ----------

# ── Record the customers that did not ──
# Shared with every other Bronze to Silver gate, so its shape and the
# projection into it live in src/common/dq.py rather than here.
quarantine_table = f"{catalog}.{silver_schema}.silver_dq_quarantine"

ensure_quarantine_table(spark, quarantine_table)
write_quarantine(flagged_customers, "bronze_customer", quarantine_table)
