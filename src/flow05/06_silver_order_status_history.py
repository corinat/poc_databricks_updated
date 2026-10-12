# Lakeflow pipeline source, not a notebook. It is listed under libraries
# in the order_status_history pipeline and evaluated by that pipeline.
#
# Flow 05, step 6: order status history as SCD Type 2, behind a DQX gate.
#
# The graph this file declares:
#
#   bronze_order_status_changes
#      │
#      ├── DQX valid ───→ valid_status_changes (view)
#      │                        │
#      │                        └── AUTO CDC ──→ silver_order_status_history
#      │
#      └── DQX invalid ──→ sink ──→ silver_dq_quarantine
#
# Two things about that shape are forced rather than chosen.
#
# The view exists because create_auto_cdc_flow takes its source as a name, not
# as a DataFrame, so there is no point at which a checked frame can be handed
# to it. A view is enough: the chain still ends in real tables on both sides,
# and materialising it instead would put a filtered copy of the Bronze feed in
# the Silver schema with no business meaning of its own.
#
# The sink exists because a pipeline cannot write outside itself. A pipeline
# source file declares a graph of datasets the pipeline owns, and the runtime
# writes them; an ordinary saveAsTable to the shared quarantine table has no
# place in it. create_sink is the declared destination that lets the rejected
# rows reach the one shared table instead of a second, pipeline-owned one.
#
# What the CDC flow produces: one row per status an order has been in, with the
# interval that status was in force. The flow adds __START_AT and __END_AT from
# the sequence_by column, so __END_AT IS NULL marks the current status.
#
# apply_as_deletes on status = 'cancelled': in SCD Type 2 a delete closes the
# open interval rather than removing rows, so a cancelled order keeps its
# history and ends up with no current row. The cancellation itself is not
# stored as a status here; silver_order carries the current status.
#
# sequence_by is sequence_num, not changed_at: the feed numbers the changes per
# order, so the integer orders them exactly, where several changes sharing a
# date would not.
#
# except_column_list drops the Bronze tracking columns. ingested_at in
# particular would otherwise sit beside __START_AT and __END_AT, which track
# when a status was in force rather than when a row was loaded. _rescued_data
# is dropped for the same reason: it records what a delivery sent that did not
# fit the Bronze schema, which is a question about the load, not about the
# order's history. It stays queryable in Bronze.

import sys

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark import pipelines as dp
from pyspark.sql.functions import expr

# COMMAND ----------

# Set by the pipeline configuration, so the catalog, schema and paths follow
# the bundle target instead of being written in here.
source_table = spark.conf.get("source_table")
order_table = spark.conf.get("order_table")
quarantine_table = spark.conf.get("quarantine_table")
checks_path = spark.conf.get("checks_path")
common_path = spark.conf.get("common_path")

# COMMAND ----------

# ── The shared quality-gate helpers ──
# src/common/dq.py is a plain module. quarantine_rows returns the projected
# frame rather than writing it, which is what this file needs: the write is the
# sink's job, not ours.
if common_path not in sys.path:
    sys.path.append(common_path)

from dq import quarantine_rows

with open(checks_path) as checks_file:
    checks = yaml.safe_load(checks_file)

dq_engine = DQEngine(WorkspaceClient())

# COMMAND ----------


def checked_changes():
    """The status change feed with DQX's result columns added.

    Read as a stream, because the AUTO CDC flow downstream needs one. The
    reference is a static table, and a stream-static anti-join is what the
    foreign key check compiles to.
    """
    return dq_engine.apply_checks_by_metadata(
        spark.readStream.table(source_table),
        checks,
        ref_dfs={"silver_order": spark.table(order_table)},
    )


# COMMAND ----------

# ── The checked feed ──
# A temporary view, not a table. The CDC flow needs its source addressable by
# name, which a view satisfies, and materialising one would mean a Silver table
# holding a filtered copy of the Bronze feed — no business meaning of its own,
# and an object in the schema that no document describes.
#
# A view over a streaming read is itself streaming, which is what AUTO CDC
# requires of its source.
@dp.temporary_view(
    name="valid_status_changes",
    comment=(
        "Status change events that passed the quality gate. The source of "
        "silver_order_status_history; rejected events are in "
        "silver_dq_quarantine."
    ),
)
def valid_status_changes():
    return dq_engine.get_valid(checked_changes())


# COMMAND ----------

# ── The rejected events ──
# A sink, not a table: the destination is the shared quarantine table, which
# this pipeline does not own.
dp.create_sink(
    name="quarantine_sink",
    format="delta",
    options={"tableName": quarantine_table},
)


@dp.append_flow(target="quarantine_sink", name="status_changes_quarantined")
def status_changes_quarantined():
    return quarantine_rows(
        dq_engine.get_invalid(checked_changes()), "bronze_order_status_changes"
    )


# COMMAND ----------

# The target has to be declared before a CDC flow can write into it. No schema
# is given: the flow derives it from the source, and a declared schema would
# have to include the __START_AT and __END_AT columns the flow manages itself.
dp.create_streaming_table(
    name="silver_order_status_history",
    comment=(
        "Status of each order over time, SCD Type 2. __END_AT IS NULL is the "
        "current status; an order with no such row was cancelled."
    ),
)

# COMMAND ----------

dp.create_auto_cdc_flow(
    name="order_status_history",
    target="silver_order_status_history",
    source="valid_status_changes",
    keys=["order_id"],
    sequence_by="sequence_num",
    apply_as_deletes=expr("status = 'cancelled'"),
    except_column_list=["source_file", "ingested_at", "_rescued_data"],
    stored_as_scd_type=2,
)
