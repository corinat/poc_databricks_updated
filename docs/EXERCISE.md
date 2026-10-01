# Databricks Source-to-Target Exercise

## Goal

Build a retail order-to-cash pipeline (Bronze → Silver → Gold), one source at a time.

Before you start, read:

- [DATA_MODEL.md](DATA_MODEL.md) — the target tables (what exists and why).
- [INGESTION_PLAN.md](INGESTION_PLAN.md) — the Bronze/Silver/Gold map and which flow builds which table.

## Setup
uv envirnoment + Databricks DAB's, use databricks free account. U have a lot of freedom, but first choice of implementation should be databricks/spark **always**.

Copy `poc/data` into a Unity Catalog volume, think about file structure.

## How to work through this exercise

Follow the flows in order, 01 through 07. Each flow is a self-contained build task with its own diagram, steps, and checks — don't skip ahead.

| Flow | Topic | File |
|---|---|---|
| 01 | Currency & exchange rate | [flows/01_currency_exchange_rate.md](flows/01_currency_exchange_rate.md) |
| 02 | Products & sellers | [flows/02_products_sellers.md](flows/02_products_sellers.md) |
| 03 | Customers | [flows/03_customers.md](flows/03_customers.md) |
| 04 | Invoices | [flows/04_invoices.md](flows/04_invoices.md) |
| 05 | Orders | [flows/05_orders.md](flows/05_orders.md) |
| 06 | Data quality, cleaning, joining | [flows/06_data_quality_cleaning_joining.md](flows/06_data_quality_cleaning_joining.md) |
| 07 | Dashboard | [flows/07_dashboard.md](flows/07_dashboard.md) |

Rule to keep in mind everywhere: don't perform business joins in Bronze. Bronze stays raw and replayable; joins happen in Gold (flow 06).

## Requirements

- Every transformation, or every small group of transformations that belong together, must end in a real, named table. No ad-hoc/inline queries that only live in a notebook cell or a temp view.
- A view is only allowed where a flow explicitly says so (e.g. the historical exchange-rate view in flow 01). Everything else is a table.
- Name each table after its layer: `bronze_<entity>`, `silver_<entity>`, `dim_<entity>`/`fact_<entity>`, `gold_datamart_<entity>`. Keep the name stable across re-runs.
- If a step needs several transformations (clean → cast → dedupe), that is still **one** output table at the end of the step — not one temp table per transformation.

## Deliverables

1. Source-to-target mapping table (see DATA_MODEL.md)
2. Bronze, Silver, and Gold table names you actually built
3. `gold_datamart_order_sales_detail` schema and join logic
4. `gold_datamart_daily_sales_summary` schema and aggregation logic
5. Final ER diagram
6. Data quality results and one quarantine example
7. Schema evolution result (flow 05)
8. Job dependency and trigger evidence (one per flow)
9. Dashboard + refresh job evidence (flow 07)



