# Target Data Model

This is a shop's order-to-cash model. One table, one job. The file format (CSV, Excel, Parquet, ...) does not matter for this diagram — only the Gold layer meaning does.

See [INGESTION_PLAN.md](INGESTION_PLAN.md) for how each table is built (Bronze → Silver → Gold), and the [flows/](flows/) folder for step-by-step guides.

```mermaid
erDiagram
    DIM_SELLER ||--o{ FACT_INVOICE : issues
    DIM_CUSTOMER ||--o{ FACT_ORDER : places
    DIM_PRODUCT ||--o{ FACT_ORDER : contains
    DIM_CURRENCY ||--o{ FACT_ORDER : uses
    DIM_CURRENCY ||--o{ FACT_EXCHANGE_RATE : identifies
    FACT_ORDER ||--o| FACT_INVOICE : billed_by
    DIM_CURRENCY ||--o{ FACT_INVOICE : billed_in
    GOLD_DATAMART_ORDER_SALES_DETAIL }o--|| DIM_CUSTOMER : enriches
    GOLD_DATAMART_ORDER_SALES_DETAIL }o--|| DIM_PRODUCT : enriches
    GOLD_DATAMART_ORDER_SALES_DETAIL }o--|| DIM_SELLER : enriches
    GOLD_DATAMART_ORDER_SALES_DETAIL }o--|| DIM_CURRENCY : enriches
    GOLD_DATAMART_ORDER_SALES_DETAIL }o--|| FACT_ORDER : built_from
    GOLD_DATAMART_ORDER_SALES_DETAIL }o--o| FACT_INVOICE : includes
    GOLD_DATAMART_ORDER_SALES_DETAIL }o--o| FACT_EXCHANGE_RATE : converts_with
    GOLD_DATAMART_DAILY_SALES_SUMMARY }o--|| GOLD_DATAMART_ORDER_SALES_DETAIL : aggregates

    DIM_SELLER {
        string seller_id PK
        string seller_name
        string tax_id
        string address
        string postal_code
        string city
        string country
        string currency_code
    }
    DIM_CUSTOMER {
        int customer_id PK
        string customer_name
        string customer_type
        string country_code
        int parent_customer_id FK
    }
    DIM_PRODUCT {
        string product_id PK
        string product_name
        string category
        decimal list_price
        string unit_of_measure
    }
    DIM_CURRENCY {
        string currency_code PK
        string currency_name
        int decimal_places
        boolean is_reporting_currency
    }
    FACT_ORDER {
        string order_id PK
        int customer_id FK
        string product_id FK
        string currency_code FK
        date order_date
        int quantity
        decimal unit_price
        decimal net_amount
        string status
    }
    FACT_EXCHANGE_RATE {
        date rate_date PK
        string currency_code PK
        decimal rate_to_pln
        string rate_source
        string table_type
    }
    FACT_INVOICE {
        string invoice_id PK
        string order_id FK
        string seller_id FK
        string currency_code FK
        date invoice_date
        decimal net_amount
        decimal vat_amount
        decimal gross_amount
        string pdf_path
        string invoice_status
    }

    GOLD_DATAMART_ORDER_SALES_DETAIL {
        string order_id PK
        date order_date
        int customer_id FK
        string product_id FK
        string seller_id FK
        string currency_code FK
        string invoice_id FK
        int quantity
        decimal net_amount
        decimal vat_amount
        decimal gross_amount
        decimal rate_to_pln
        decimal order_amount_pln
        string order_status
    }

    GOLD_DATAMART_DAILY_SALES_SUMMARY {
        date sales_date PK
        string currency_code PK
        string product_category PK
        int order_count
        int item_quantity
        decimal net_sales
        decimal gross_sales
        decimal sales_pln
    }
```

## Which file feeds which table

| Table | Purpose | Source file(s) | Flow |
|---|---|---|---|
| `dim_seller` | Legal seller that issues invoices | `sellers_delta/` | 02 |
| `dim_product` | Product catalog and list prices | `products_excel.xlsx` | 02 |
| `dim_customer` | Customer accounts (holding company → account → branch hierarchy via `parent_customer_id`) | `customers.csv` + `customers_dirty.csv` | 03 |
| `dim_currency` | Approved currency reference data | `currencies.csv` | 01 |
| `fact_order` | Sales transactions | `orders.parquet` + `orders_stream.parquet` + `orders_schema_change.parquet` | 05 |
| `silver_order_status_history` | Order status over time (not in the ER diagram above) | `order_status_changes.parquet` | 05 |
| `fact_exchange_rate` | Daily currency conversion rates | `exchange_rates_historical.csv` + NBP API | 01 |
| `fact_invoice` | Invoice documents and extracted fields | `invoices/*.pdf` | 04 |
| `gold_datamart_order_sales_detail` | One joined row per order | Built from the Gold tables above | 06 |
| `gold_datamart_daily_sales_summary` | Daily aggregated sales for the dashboard | Built from `gold_datamart_order_sales_detail` | 07 |

## Rules

1. Dimensions = things (customer, product, seller, currency). Facts = events (order, invoice, rate).
2. One business meaning → one table, even if the file format changes.
3. Bronze keeps `source_file` and `ingested_at` on every row.
4. Exchange rates are the only table that mixes a CSV history with a live API feed.

## How the two marts are built

| Mart | Built from | One row per |
|---|---|---|
| `gold_datamart_order_sales_detail` | `fact_order` + `dim_customer` + `dim_product` + `dim_currency` + `dim_seller` + `fact_invoice` + `fact_exchange_rate` | `order_id` |
| `gold_datamart_daily_sales_summary` | `gold_datamart_order_sales_detail`, grouped by `order_date` + `currency_code` + `product_category` | one row per day/currency/category |

See Flow 06 for the join diagram, and Flow 07 for the dashboard built on top of these marts.
