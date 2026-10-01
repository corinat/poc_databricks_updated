# Source-to-Target Ingestion Plan

One page map: which file goes where, and which flow builds it. Step-by-step details live in [flows/](flows/).

## Flow index

| Flow | Topic | Bronze | Silver | Gold |
|---|---|---|---|---|
| [01](flows/01_currency_exchange_rate.md) | Currency & exchange rate | `bronze_currency`, `bronze_exchange_rate_current`, view `bronze_exchange_rate_hist` | `silver_exchange_rate` | `dim_currency`, `fact_exchange_rate` |
| [02](flows/02_products_sellers.md) | Products & sellers | `bronze_product`, `bronze_seller` | — (published as-is) | `dim_product`, `dim_seller` |
| [03](flows/03_customers.md) | Customers | `bronze_customer` | `silver_customer` (recursive CTE: level/path/root) | `dim_customer` |
| [04](flows/04_invoices.md) | Invoices | `bronze_invoice` (VARIANT) | `silver_invoice` | `fact_invoice` |
| [05](flows/05_orders.md) | Orders | `bronze_order` | `silver_order` (SCD1), `silver_order_status_history` (SCD2) | `fact_order` |
| [06](flows/06_data_quality_cleaning_joining.md) | Data quality, cleaning, joining | — | applies to every Silver table above | `gold_datamart_order_sales_detail`, `gold_datamart_sales_by_currency`, `gold_datamart_daily_metrics` |
| [07](flows/07_dashboard.md) | Dashboard | — | — | `gold_datamart_daily_sales_summary` → dashboard |

## Source mapping

```mermaid
flowchart LR
    DELTA[Delta\nseller master] --> B2[bronze_seller]
    XLSX[Excel\nproduct catalog] --> B1[bronze_product]
    CSV1[CSV\ncustomers] --> B3[bronze_customer]
    CSV2[CSV\ncurrencies] --> B4[bronze_currency]
    PARQUET[Parquet\norder files] --> B5[bronze_order]
    CDC[Parquet\norder status changes] --> B5
    API[CSV history + NBP API\nexchange rates] --> B6[bronze_exchange_rate_current /\nbronze_exchange_rate_hist]
    PDF[PDF\nVAT invoices] --> B7[bronze_invoice]

    B2 --> G2[dim_seller]
    B1 --> G1[dim_product]
    B3 --> S3[silver_customer] --> G3[dim_customer]
    B4 --> G4[dim_currency]
    B5 --> S5[silver_order] --> G5[fact_order]
    B5 --> S5b[silver_order_status_history]
    B6 --> S6[silver_exchange_rate] --> G6[fact_exchange_rate]
    B7 --> S7[silver_invoice] --> G7[fact_invoice]

    G1 & G2 & G3 & G4 & G5 & G6 & G7 --> DETAIL[gold_datamart_order_sales_detail]
    DETAIL --> SUMMARY[gold_datamart_daily_sales_summary] --> DASH[Dashboard]
```

## Medallion architecture

```mermaid
flowchart LR
    subgraph BRONZE[Bronze: raw and traceable]
        B1[bronze_seller]
        B2[bronze_product]
        B3[bronze_customer]
        B4[bronze_currency]
        B5[bronze_order]
        B6[bronze_exchange_rate_current /\nbronze_exchange_rate_hist]
        B7[bronze_invoice]
    end

    subgraph SILVER[Silver: clean and conformed]
        V3[silver_customer]
        V5[silver_order]
        V5b[silver_order_status_history]
        V6[silver_exchange_rate]
        V7[silver_invoice]
    end

    subgraph GOLD[Gold: joined marts]
        G0[dim_seller / dim_product /\ndim_customer / dim_currency /\nfact_order / fact_exchange_rate /\nfact_invoice]
        G1[gold_datamart_order_sales_detail]
        G2[gold_datamart_daily_sales_summary]
    end

    B1 --> G0
    B2 --> G0
    B3 --> V3 --> G0
    B4 --> G0
    B5 --> V5 --> G0
    B5 --> V5b
    B6 --> V6 --> G0
    B7 --> V7 --> G0
    G0 --> G1 --> G2
```

Bronze stays close to the source and is replayable. Silver is clean, typed, and deduplicated. Gold is business-ready for reporting.

## Quality gates (Flow 06 covers this in depth, using DQX)

- Dimensions: unique keys, required names, valid tax IDs, valid product prices, approved currencies.
- Orders: unique order IDs, valid customer/product keys, positive quantity, non-negative amount.
- Exchange rates: unique `(effectiveDate, code)`, positive `mid`, supported currency.
- Invoices: unique invoice ID, valid order ID, matching seller/buyer, and `net + VAT = gross`.
- Every Bronze table: keep `source_file`, `ingested_at`, and quarantine status.

## Load decision

```mermaid
flowchart TD
    NEW[New source] --> FORMAT{Business source}
    FORMAT -->|Historical order file| COPY[COPY INTO or batch read]
    FORMAT -->|New order files| AUTO[Auto Loader]
    FORMAT -->|Continuous order events| STREAM[Structured Streaming]
    FORMAT -->|NBP rates| API[Python task calls API]
    FORMAT -->|Invoice PDF| PDF[Extract and validate]
    COPY --> WRITE{Write mode}
    AUTO --> WRITE
    STREAM --> WRITE
    API --> WRITE
    PDF --> WRITE
    WRITE --> APPEND[append new records]
    WRITE --> PART[overwrite selected partition]
    WRITE --> TABLE[overwrite full table]
    APPEND --> EVOLVE{Schema change?}
    PART --> EVOLVE
    TABLE --> EVOLVE
    EVOLVE -->|Add compatible columns| MERGE[mergeSchema]
    EVOLVE -->|Replace schema intentionally| OVER[overwriteSchema]
    EVOLVE -->|No change| KEEP[keep schema]
```
