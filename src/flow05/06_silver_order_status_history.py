# Lakeflow pipeline source, not a notebook task. It is listed under libraries
# in the order_status_history pipeline and evaluated by that pipeline.
#
# Flow 05, step 6: order status history as SCD Type 2.
#
# Source: bronze_order_status_changes, the landed status change feed. The CDC
# flow reads it as a stream, so that Bronze table has to stay append-only.
#
# What the flow produces: one row per status an order has been in, with the
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

from pyspark import pipelines as dp
from pyspark.sql.functions import expr

# COMMAND ----------

# Set by the pipeline configuration, so the catalog and schema follow the
# bundle target instead of being written in here.
source_table = spark.conf.get("source_table")

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
    source=source_table,
    keys=["order_id"],
    sequence_by="sequence_num",
    apply_as_deletes=expr("status = 'cancelled'"),
    except_column_list=["source_file", "ingested_at", "_rescued_data"],
    stored_as_scd_type=2,
)
