"""The four transformations that are exercised by tests.

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

Three of the four take DataFrames. customer_hierarchy takes a name instead,
because it is the one that cannot be written in the DataFrame API: a recursive
CTE has no equivalent there, and expressing the walk as a loop that joins until
no new rows appear would be a different algorithm that also loses the recursion
limit Spark uses to stop a cycle. Its name is interpolated into SQL, and is
always one the bundle computes or a test defines.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def customer_hierarchy(
    spark: SparkSession,
    source: str,
    country_expr: str,
) -> DataFrame:
    """Walk the customer parent links and place each customer in its tree.

    Deliveries are deduplicated to one row per customer first, newest batch
    winning, so a customer redelivered with a new parent is walked from its
    latest known position.

    A cycle in `parent_customer_id` would recurse forever. Spark's recursion
    limit stops it and fails the query instead, when the returned frame is
    evaluated rather than when this function is called.

    Args:
        spark: Session the recursive query runs on.
        source: Table or view holding the Bronze customer deliveries.
        country_expr: SQL expression for the country column. Bronze is written
            with `mergeSchema`, so whether `country`, `country_code` or both
            exist depends on which batches have arrived; the caller builds the
            expression from the columns actually present.

    Returns:
        One row per `customer_id`, holding `customer_id`, `customer_name`,
        `country_code`, `parent_customer_id`, `level`, `hierarchy_path`,
        `root_customer_id`, `source_file` and `ingested_at`. A customer whose
        parent is absent from the source is returned as a root — `level` 0
        with `parent_customer_id` still set — rather than dropped.
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
    fact_order: DataFrame,
    fact_invoice: DataFrame,
    fact_exchange_rate: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
    dim_currency: DataFrame,
    dim_seller: DataFrame,
) -> DataFrame:
    """Join the star schema into the detail mart flow 06 section 3 specifies.

    Two of the six joins can multiply rows, and both are reduced to one row per
    order before they are joined rather than checked afterwards:

      invoice_per_order   `fact_invoice` is one row per `invoice_id` and
                          nothing stops two invoices from carrying the same
                          `order_id`. The latest `invoice_date` wins, then the
                          highest `invoice_id` breaks a tie.

      rate_per_order      the rate is matched on a range, so every rate
                          published up to the order date qualifies. The latest
                          of them wins, which is the most recent rate on or
                          before the order date — never one published after the
                          order happened.

    The reporting currency is the exception to needing a published rate: NBP
    does not quote the zloty against itself, so its rate is 1 by definition,
    taken from `dim_currency`'s `is_reporting_currency` flag rather than from
    the literal 'PLN'.

    Args:
        fact_order: Order facts, one row per order. This frame drives the row
            count; the six joins are all left joins onto it.
        fact_invoice: Invoice facts, one row per `invoice_id`. Contributes
            `invoice_id`, `seller_id`, `vat_amount` and `gross_amount`.
        fact_exchange_rate: Published rates, matched on currency and on a date
            range rather than on equality.
        dim_customer: Customer dimension. Contributes no column — it is joined
            because section 3's table lists it, and every key it would resolve
            is already carried by the order. A duplicated `customer_id` here
            would multiply rows, which flow 06's check_marts task catches.
        dim_product: Product dimension. Contributes `category`.
        dim_currency: Currency dimension. Contributes `is_reporting_currency`,
            which decides the implied rate above.
        dim_seller: Seller dimension, reached through the invoice rather than
            the order. Contributes no column, on the same terms as
            `dim_customer`.

    Returns:
        One row per row of `fact_order`, holding `order_id`, `order_date`,
        `customer_id`, `product_id`, `seller_id`, `currency_code`,
        `product_category`, `invoice_id`, `quantity`, `net_amount`,
        `vat_amount`, `gross_amount`, `rate_to_pln`, `order_amount_pln` and
        `order_status`. `seller_id`, `invoice_id`, `vat_amount` and
        `gross_amount` are NULL for an order no invoice refers to;
        `rate_to_pln` and `order_amount_pln` are NULL for an order in a
        currency with no rate published on or before its date, rather than
        falling back to the unconverted amount.
    """
    one_invoice = Window.partitionBy("order_id").orderBy(
        F.col("invoice_date").desc(), F.col("invoice_id").desc()
    )
    invoice_per_order = (
        fact_invoice
        .filter(F.col("order_id").isNotNull())
        .withColumn("_rn", F.row_number().over(one_invoice))
        .filter(F.col("_rn") == 1)
        .select("order_id", "invoice_id", "seller_id", "vat_amount", "gross_amount")
    )

    # An inner join here and a left join below, so an order with no rate keeps
    # its row and gets a NULL.
    latest_rate = Window.partitionBy("order_id").orderBy(F.col("rate_date").desc())
    rate_per_order = (
        fact_order.alias("o")
        .join(
            fact_exchange_rate.alias("r"),
            (F.col("r.currency_code") == F.col("o.currency_code"))
            & (F.col("r.rate_date") <= F.col("o.order_date")),
            "inner",
        )
        .select(
            F.col("o.order_id").alias("order_id"),
            F.col("r.rate_date").alias("rate_date"),
            F.col("r.rate_to_pln").alias("rate_to_pln"),
        )
        .withColumn("_rn", F.row_number().over(latest_rate))
        .filter(F.col("_rn") == 1)
        .select("order_id", "rate_to_pln")
    )

    joined = (
        fact_order.alias("o")
        .join(dim_customer.alias("cu"),
            F.col("o.customer_id") == F.col("cu.customer_id"), "left")
        .join(dim_product.alias("p"),
            F.col("o.product_id") == F.col("p.product_id"), "left")
        .join(dim_currency.alias("c"),
            F.col("o.currency_code") == F.col("c.currency_code"), "left")
        .join(invoice_per_order.alias("i"),
            F.col("o.order_id") == F.col("i.order_id"), "left")
        .join(dim_seller.alias("s"),
            F.col("i.seller_id") == F.col("s.seller_id"), "left")
        .join(rate_per_order.alias("rt"),
            F.col("o.order_id") == F.col("rt.order_id"), "left")
        .select(
            F.col("o.order_id").alias("order_id"),
            F.col("o.order_date").alias("order_date"),
            F.col("o.customer_id").alias("customer_id"),
            F.col("o.product_id").alias("product_id"),
            F.col("i.seller_id").alias("seller_id"),
            F.col("o.currency_code").alias("currency_code"),
            F.col("p.category").alias("product_category"),
            F.col("i.invoice_id").alias("invoice_id"),
            F.col("o.quantity").alias("quantity"),
            F.col("o.net_amount").alias("net_amount"),
            F.col("i.vat_amount").alias("vat_amount"),
            F.col("i.gross_amount").alias("gross_amount"),
            # The reporting currency converts at 1; everything else needs a
            # published rate.
            F.coalesce(
                F.col("rt.rate_to_pln"),
                F.when(F.col("c.is_reporting_currency"),
                    F.lit(1).cast("decimal(18,6)")),
            ).alias("rate_to_pln"),
            F.col("o.status").alias("order_status"),
        )
    )

    # order_amount_pln is the delivered column flow 06 asks the rate join to
    # produce. NULL when no rate applies, rather than silently falling back to
    # the net amount.
    return joined.select(
        "order_id", "order_date", "customer_id", "product_id", "seller_id",
        "currency_code", "product_category", "invoice_id", "quantity",
        "net_amount", "vat_amount", "gross_amount", "rate_to_pln",
        F.round(F.col("net_amount") * F.col("rate_to_pln"), 2)
        .cast("decimal(18,2)").alias("order_amount_pln"),
        "order_status",
    )


def cleaned_orders(orders_df: DataFrame) -> DataFrame:
    """Reduce the Bronze order deliveries to one typed row per order.

    The three deliveries genuinely repeat rows — the stream replays the last
    eight orders and the schema-change delivery replays the first eight, both
    already in the batch file — so Bronze holds more rows than orders and this
    is where that is resolved.

    Which copy survives: the most recent `ingested_at`, then the later
    `order_date`. That ordering is what makes the schema-change delivery win
    for the orders it replays, and so what gives those rows a `sales_channel`.
    Flow 05 asks for the widened column to be demonstrated, and it is
    demonstrated there; reverse the ordering and the column is empty on every
    row, with nothing failing, because `sales_channel` is not in FACT_ORDER
    and not in any rule.

    The window is applied after the select, so it orders by the cast
    `order_date` rather than the Bronze string.

    Args:
        orders_df: Bronze order deliveries, which repeat rows and carry every
            column as a string.

    Returns:
        One row per distinct `order_id`, holding `order_id`, `customer_id`,
        `product_id`, `currency_code`, `order_date`, `quantity`, `unit_price`,
        `net_amount`, `status`, `sales_channel`, `source_file` and
        `ingested_at`, cast to the data model's types. `status` is lowercased
        and trimmed; `currency` and `order_amount` are renamed to
        `currency_code` and `net_amount`.
    """
    dedup = Window.partitionBy("order_id").orderBy(
        F.col("ingested_at").desc(), F.col("order_date").desc()
    )

    return (
        orders_df.select(
            "order_id",
            F.col("customer_id").cast("int").alias("customer_id"),
            "product_id",
            F.col("currency").alias("currency_code"),
            F.col("order_date").cast("date").alias("order_date"),
            F.col("quantity").cast("int").alias("quantity"),
            F.col("unit_price").cast("decimal(18,2)").alias("unit_price"),
            F.col("order_amount").cast("decimal(18,2)").alias("net_amount"),
            F.lower(F.trim(F.col("status"))).alias("status"),
            "sales_channel",
            "source_file",
            "ingested_at",
        )
        .withColumn("_rn", F.row_number().over(dedup))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def combined_rates(
    current_df: DataFrame,
    hist_df: DataFrame,
    columns: list[str],
) -> DataFrame:
    """Union the two rate sources into one row per rate date and currency.

    Each source is tagged twice at the point they are combined, which is the
    one place the distinction cannot be mistaken. `source_priority` orders the
    dedupe and is dropped; `rate_source` is kept.

    The API wins a date both sources carry, because it is the bank's own
    publication and the CSV is a copy. Reverse the priority and every
    overlapping date is labelled as coming from the CSV and converted at the
    CSV's rate, with no rule to notice: no check mentions `rate_source`, and
    nothing downstream reads it.

    Args:
        current_df: Rates fetched from the NBP API.
        hist_df: Rates read from the historical CSV. Carries the same columns
            as `current_df`, which is what lets the two be unioned by name.
        columns: Silver column order to select, so the frame cannot drift from
            the table it is written to.

    Returns:
        One row per `effectiveDate` and `code`, holding exactly `columns` in
        that order. `rate_source` is added here, so it reaches the result only
        because the caller lists it; `source_priority` is added for the dedupe
        and never selected. Where both sources publish a date, the API row
        survives; within one source, the latest `ingested_at` does.
    """
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
