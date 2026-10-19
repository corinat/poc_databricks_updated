"""The two transformations that are exercised by tests.

Both are plain functions over table names rather than code inside a notebook,
for one reason: a notebook reads widgets at import time and writes to a real
table, so nothing in it can be called with inputs of your choosing. Here the
caller supplies the names, so a notebook passes catalog-qualified tables and a
test passes temporary views over frames it built in memory. The SQL is the same
either way.

Four transformations are extracted, and the test for each covers logic that
either never executes on the sample data or lands in a column nothing
downstream and no DQX rule would notice going wrong:

  customer_hierarchy   the recursive walk that produces level, hierarchy_path
                       and root_customer_id. No DQX rule checks those three
                       columns and none of them reaches Gold, so a regression
                       is invisible everywhere.

  order_sales_detail   the row resolution behind the joined mart: one invoice
                       per order, the most recent rate on or before the order
                       date, and the reporting currency's implied rate. The
                       sample data has a rate for every order date and no PLN
                       orders, so two of those branches never run.

  cleaned_orders       the rename, the casts, and the dedupe that decides which
                       delivery of a replayed order survives. The decision shows
                       up in sales_channel, which stops at Silver and appears in
                       no rule, so picking the wrong delivery is silent.

  combined_rates       the union of the two rate sources and the dedupe that
                       prefers the bank's own publication over the historical
                       copy. The decision shows up in rate_source, which no rule
                       checks and nothing reads — and in which rate is used.

Everything else in Silver and Gold is either asserted by a DQX rule, checked at
runtime by flow 06's check_marts, or a projection whose shape is already pinned
in DDL.

Both functions build SQL by interpolating names. They are called with names the
bundle computes or a test defines, never with anything a user supplies.
"""


def customer_hierarchy(spark, source: str, country_expr: str):
    """One row per customer, with its place in the hierarchy.

    Args:
        spark: the session.
        source: table or view holding the Bronze customer deliveries.
        country_expr: SQL for the country column. Bronze is written with
            mergeSchema, so whether `country`, `country_code` or both exist
            depends on which batches have arrived; the caller builds the
            expression from the columns actually present.

    Returns a frame with customer_id, customer_name, country_code,
    parent_customer_id, level, hierarchy_path, root_customer_id, source_file
    and ingested_at.

    A cycle in parent_customer_id would recurse forever; Spark's recursion
    limit stops it and fails the query instead.
    """
    return spark.sql(f"""
WITH RECURSIVE cleaned AS (
    SELECT
        CAST(trim(customer_id) AS INT)                    AS customer_id,
        trim(customer_name)                               AS customer_name,
        upper(trim({country_expr}))                       AS country_code,
        CAST(nullif(trim(parent_customer_id), '') AS INT) AS parent_customer_id,
        source_file,
        ingested_at
    FROM {source}
),

-- One row per customer_id, newest batch first.
deduped AS (
    SELECT customer_id, customer_name, country_code, parent_customer_id,
           source_file, ingested_at
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
        customer_id   AS root_customer_id,
        source_file,
        ingested_at
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
        parent.root_customer_id,
        child.source_file,
        child.ingested_at
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
    root_customer_id,
    source_file,
    ingested_at
FROM hierarchy
""")


def order_sales_detail(
    spark,
    fact_order: str,
    fact_invoice: str,
    fact_exchange_rate: str,
    dim_customer: str,
    dim_product: str,
    dim_currency: str,
    dim_seller: str,
):
    """One row per order, with the six left joins flow 06 section 3 specifies.

    Args are table or view names, one per source.

    Two of the six joins can multiply rows, and both are reduced to one row per
    order before they are joined rather than checked afterwards:

      invoice_per_order   fact_invoice is one row per invoice_id and nothing
                          stops two invoices from carrying the same order_id.
                          The latest invoice_date wins, then the highest
                          invoice_id breaks a tie.

      rate_per_order      the rate is matched on a range, so every rate
                          published up to the order date qualifies. The latest
                          of them wins.

    The reporting currency is the exception to needing a published rate: NBP
    does not quote the zloty against itself, so its rate is 1 by definition,
    taken from dim_currency's is_reporting_currency flag rather than from the
    literal 'PLN'.
    """
    return spark.sql(f"""
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


def cleaned_orders(spark, source: str):
    """One row per order_id, renamed and cast to the data model.

    Args:
        spark: the session.
        source: table or view holding the Bronze order deliveries.

    The three deliveries genuinely repeat rows — the stream replays the last
    eight orders and the schema-change delivery replays the first eight, both
    already in the batch file — so Bronze holds more rows than orders and this
    is where that is resolved.

    Which copy survives: the most recent ingested_at, then the later
    order_date. That ordering is what makes the schema-change delivery win for
    the orders it replays, and so what gives those rows a sales_channel. Flow
    05 asks for the widened column to be demonstrated, and it is demonstrated
    there; reverse the ordering and the column is empty on every row, with
    nothing failing, because sales_channel is not in FACT_ORDER and not in any
    rule.
    """
    return spark.sql(f"""
SELECT
    order_id,
    CAST(customer_id AS INT)            AS customer_id,
    product_id,
    currency                            AS currency_code,
    CAST(order_date AS DATE)            AS order_date,
    CAST(quantity AS INT)               AS quantity,
    CAST(unit_price AS DECIMAL(18,2))   AS unit_price,
    CAST(order_amount AS DECIMAL(18,2)) AS net_amount,
    lower(trim(status))                 AS status,
    sales_channel,
    source_file,
    ingested_at
FROM {source}
QUALIFY row_number() OVER (
    PARTITION BY order_id
    ORDER BY ingested_at DESC, order_date DESC
) = 1
""")


def combined_rates(current_df, hist_df, columns: list[str]):
    """One row per effectiveDate and code, from the two rate sources.

    Args:
        current_df: the rates fetched from the NBP API.
        hist_df: the rates read from the historical CSV.
        columns: the Silver column order to select, so the frame cannot drift
            from the table it is written to.

    Each source is tagged twice at the point they are combined, which is the
    one place the distinction cannot be mistaken. source_priority orders the
    dedupe and is dropped; rate_source is kept.

    The API wins a date both sources carry, because it is the bank's own
    publication and the CSV is a copy. Reverse the priority and every
    overlapping date is labelled as coming from the CSV and converted at the
    CSV's rate, with no rule to notice: no check mentions rate_source, and
    nothing downstream reads it.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    combined = (
        current_df
        .withColumn("source_priority", F.lit(1))
        .withColumn("rate_source", F.lit("NBP_API"))
        .unionByName(
            hist_df
            .withColumn("source_priority", F.lit(2))
            .withColumn("rate_source", F.lit("HISTORICAL_CSV"))
        )
    )

    dedup_order = Window.partitionBy("effectiveDate", "code").orderBy(
        F.col("source_priority").asc(),
        F.col("ingested_at").desc(),
    )

    return (
        combined
        .withColumn("_rn", F.row_number().over(dedup_order))
        .filter(F.col("_rn") == 1)
        .select(*columns)
    )
