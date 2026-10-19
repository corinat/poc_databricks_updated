"""Shared pieces of the DQX quality gate at the Bronze to Silver boundary.

Not a notebook. The gate notebooks append this directory to sys.path from their
common_path job parameter and import from here.

What belongs in this module is the part that is identical for every gate: the
shared quarantine table's shape, and the projection of DQX's output into it.
What does not belong here is anything a gate decides for itself — its rules,
its reference DataFrames, and how it writes the rows that passed, which is a
merge in one flow and a full overwrite in another.
"""

# The columns DQX appends to the frames it returns. Excluded from the payload,
# because they are the report rather than the row. _dq_info appears only when
# certain checks are used, so it is listed here rather than assumed absent.
RESULT_COLUMNS = ("_errors", "_warnings", "_dq_info")

# One table for every source, as flow 06 requires. The failing row goes in
# whole as VARIANT, because the sources have different columns and one shared
# table cannot give each a typed schema.
#
# failed_checks carries only the rule name and its message. DQX's own result
# struct has ten fields, most of them its bookkeeping, and pinning that shape
# in DDL would recouple this table to the library version.
#
# CREATE TABLE IF NOT EXISTS, because every gate writes here and none of them
# owns the table. The jobs are independent per flow, so any one of them may be
# the first to run on a fresh workspace.
_QUARANTINE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    source_table   STRING NOT NULL,
    severity       STRING,
    row_data       VARIANT,
    failed_checks  ARRAY<STRUCT<name STRING, message STRING>>,
    quarantined_at TIMESTAMP
)
"""

# _errors and _warnings are NULL rather than empty when a row has none of that
# kind, which is why the two are combined by CASE rather than by concat —
# concat of a NULL array returns NULL.
_FAILED_CHECKS_EXPR = """
transform(
    CASE
        WHEN _errors IS NULL   THEN _warnings
        WHEN _warnings IS NULL THEN _errors
        ELSE concat(_errors, _warnings)
    END,
    e -> named_struct('name', e.name, 'message', e.message)
)
"""


def ensure_quarantine_table(spark, table: str) -> None:
    """Create the shared quarantine table if it is not there yet."""
    spark.sql(_QUARANTINE_DDL.format(table=table))


def require_references(ref_dfs: dict) -> None:
    """Fail if any reference frame is empty, before a gate reads it.

    A foreign_key check cannot tell "this value is wrong" from "the reference
    table was never loaded" — it only reports "not found". So an empty
    reference makes every row fail that rule, and what happens next depends on
    the rule's criticality and the gate's write mode. Where the rule is an
    error and the write can delete, the result is an emptied Silver table from a
    run that looks successful.

    This turns that into a failure that names the table instead. The way a
    reference goes quietly empty is a load that read nothing: an Auto Loader
    stream finding no files in its folder is not an error, so the task reports
    success and the table it was meant to fill stays as it was.

    Worth calling only from a gate whose foreign keys are errors. Where they
    are warnings an empty reference flags every row but costs no data, so the
    check would only add noise.
    """
    empty = sorted(name for name, df in ref_dfs.items() if df.isEmpty())
    if empty:
        raise RuntimeError(
            "Reference tables are empty, so every foreign key check would "
            "fail and the gate would reject every row: "
            + ", ".join(empty)
            + ". Load them and re-run."
        )


def require_rows_to_write(valid_df, target: str) -> None:
    """Fail if a gate passed nothing, before a write that deletes.

    A per-row DQX rule cannot express this. Each rule answers a question about
    one row and routes it to valid or to the quarantine; whether the valid set
    came out empty is a property of the result, so it is checked here.

    It matters where the write removes rows the source no longer carries. An
    empty valid set then deletes the whole table, from a run that reports
    success — and an emptied Silver table empties the Gold table overwritten
    from it, and the marts built on that.

    require_references covers one way to arrive here: a reference table that is
    empty, so every foreign key check fails. This covers the rest, which that
    one cannot see — a truncated source, a rule that starts rejecting
    everything, or a reference table that is present but holds the wrong
    values, such as a currency list replaced by one that names none of the
    currencies in use.

    An empty valid set is never a state worth writing: either there was nothing
    to load, or the gate rejected all of it, and neither is a reason to empty
    the target.

    Called from six of the seven gates: the five that write with INSERT
    OVERWRITE and the one that merges with a delete clause. Not from
    silver_exchange_rate, which writes with replaceUsing — that deletes only
    the dates the incoming frame carries, so an empty frame deletes nothing and
    the table it would protect is already safe.
    """
    if valid_df.isEmpty():
        raise RuntimeError(
            f"The quality gate passed no rows, so writing to {target} would "
            "delete every row it holds. Either the source is empty or every "
            "row failed a check: the reasons are in the quarantine for this "
            "run. Nothing was written."
        )


def quarantine_rows(flagged_df, source_table: str):
    """Project DQX's flagged rows into the shared quarantine table's shape.

    Returns the frame rather than writing it, because the two callers write
    differently: a notebook appends it to the table, while a pipeline cannot
    write outside itself and has to route it into a sink instead.

    flagged_df holds rows that failed an error rule and rows that failed a warn
    rule. severity is derived from which result column was populated: an error
    row is in the quarantine instead of in Silver, a warn row is in both.

    The payload columns are derived from the frame rather than listed by the
    caller, so a gate cannot forget one. Every name is backticked, because at
    least one source column is called `table`.
    """
    from pyspark.sql import functions as F

    payload = ", ".join(
        f"`{c}`" for c in flagged_df.columns if c not in RESULT_COLUMNS
    )

    return flagged_df.select(
        F.lit(source_table).alias("source_table"),
        F.when(F.col("_errors").isNotNull(), F.lit("error"))
        .otherwise(F.lit("warn"))
        .alias("severity"),
        F.expr(f"parse_json(to_json(struct({payload})))").alias("row_data"),
        F.expr(_FAILED_CHECKS_EXPR).alias("failed_checks"),
        F.current_timestamp().alias("quarantined_at"),
    )


def write_quarantine(flagged_df, source_table: str, table: str) -> None:
    """Append DQX's flagged rows to the shared quarantine table.

    For the notebook gates. A pipeline uses quarantine_rows and a sink.
    """
    (
        quarantine_rows(flagged_df, source_table)
        .write.mode("append")
        .saveAsTable(table)
    )
