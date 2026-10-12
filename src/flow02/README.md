# Flow 02 — Products & Sellers

Two independent Bronze loads, one file to one table, each followed by a Silver build that types the data and resolves re-deliveries.

Requirements: [flows/flows/02_products_sellers.md](../../flows/flows/02_products_sellers.md), plus the general rules in [docs/EXERCISE.md](../../docs/EXERCISE.md), [docs/DATA_MODEL.md](../../docs/DATA_MODEL.md) and [docs/INGESTION_PLAN.md](../../docs/INGESTION_PLAN.md).

## What gets built

| Layer | Object | Type | Source | Write mode | Notebook |
|---|---|---|---|---|---|
| Bronze | `bronze_product` | table | `products_excel.xlsx` | append | `01_load_bronze_product.py` |
| Bronze | `bronze_seller` | table | `sellers_delta/` | append | `02_load_bronze_seller.py` |
| Silver | `silver_product` | table | `bronze_product` | overwrite | `03_build_silver_product.py` |
| Silver | `silver_seller` | table | `bronze_seller` | overwrite | `04_build_silver_seller.py` |
| Silver | `silver_dq_quarantine` | table | rows the quality gates reject | append | both Silver notebooks |

Bronze objects live in schema `bronze_raw` and Silver in `silver`, in the catalog chosen by the bundle target (`poc_dev`, `poc_test`, `poc`). In the `dev` target, development mode prefixes schema names with `dev_<user>_`.

```mermaid
flowchart LR
    XLSX[products_excel.xlsx] -->|file arrival| B1[bronze_product] --> DQ1{DQX} -->|passed| S1[silver_product]
    DELTA[sellers_delta/] -->|nightly schedule| B2[bronze_seller] --> DQ2{DQX} -->|passed| S2[silver_seller]
    DQ1 -->|flagged| Q[silver_dq_quarantine]
    DQ2 -->|flagged| Q
```

The two Bronze loads share nothing, and the flow's own point is the **triggers**, since the sources arrive differently. The two Silver builds do share something: both write to `silver_dq_quarantine`, and `silver_seller`'s gate reads `bronze_currency` to check the seller's currency against the approved list.

## Jobs and triggers

Defined in [resources/flow02_product_job.job.yml](../../resources/flow02_product_job.job.yml) and [resources/flow02_seller_job.job.yml](../../resources/flow02_seller_job.job.yml). Both use notebook tasks only, on serverless environment version 6.

| Job | Tasks | Trigger | Why |
|---|---|---|---|
| `load_bronze_product` | `load_bronze_product` → `build_silver_product` | file arrival on `data/products_excel/` | The catalog only changes when a new file lands, so react to the event |
| `load_bronze_seller` | `load_bronze_seller` → `build_silver_seller` | schedule, nightly 02:00 UTC | Nothing pushes to the Delta folder, so poll instead |

Each Silver build depends on its own Bronze load, so a delivery that failed to land is not silently left out of Silver.

One consequence of the seller job's schedule: its gate now runs nightly. The quarantine is a journal rather than a current-state table, so if the tax id or the currency ever fails a check, a row is appended every night until it is fixed.

Development mode normally pauses triggers and schedules. Both job files set `pause_status: UNPAUSED` explicitly so the triggers stay active in dev too.

## Approach per job

### Job A — `bronze_product`

- **Auto Loader** over the products folder with the **native Excel reader**: `cloudFiles.format = "excel"`. No external package — the reader is built into the runtime (Databricks Runtime 17.1+).
- `pathGlobFilter = "*.xlsx"` restricts the stream to workbooks. This is the only place extension filtering can happen: a file arrival trigger path cannot contain a wildcard and the trigger has no filter option of its own.
- `headerRows = 1` — the sheet has one header row, which is also the maximum the reader supports.
- Explicit all-`STRING` schema, no inference. Nothing is cast: `unit_price` becomes a decimal and is renamed to `list_price` further downstream.
- Adds `source_file` from `_metadata.file_name` and `ingested_at`.
- `trigger(availableNow=True)` processes whatever is waiting and stops, so the task finishes rather than running as a continuous stream.
- Append, with Auto Loader's checkpoint deciding what is new. This is what makes "a new file is a new delivery" work: the trigger does not say which file arrived, so the checkpoint is what distinguishes a new delivery from one already ingested. Deliveries can have any file name, several can be waiting at once, and a run that fires without a new file processes nothing.

### Job B — `bronze_seller`

- `spark.read.format("delta")` over the folder. The Delta schema is the Bronze schema; nothing is cast.
- Adds `source_file` (the folder name) and `ingested_at`.
- Append. Re-runs accumulate rows; deduplication happens downstream, not in Bronze.

### Job A, step 2 — `silver_product`

- Casts and renames to `DIM_PRODUCT`: `unit_price` becomes `list_price` and becomes a `DECIMAL(18,2)`. That one change is why the table exists — Silver is where this project casts, and letting Gold do it would make Gold the only place that does.
- Dedupes to one row per `product_id`, newest `ingested_at` first, so a re-delivered workbook replaces the catalogue rather than doubling it.
- `unit_of_measure` is in `DIM_PRODUCT` and not here: the sheet has four columns and none of them is a unit.
- The schema is declared with `CREATE TABLE IF NOT EXISTS` and filled with `insertInto` in overwrite mode, which writes into the existing table rather than replacing it, so the declared schema survives.

### Job B, step 2 — `silver_seller`

- Nothing is cast or renamed: `bronze_seller`'s columns are already the ones `DIM_SELLER` wants, all `STRING`, and the model wants them all `STRING`.
- The dedupe is the whole reason the table exists. `bronze_seller` is appended **in full** on every load, so the same seller is present once per run, and `source_file` is the folder name rather than a file name so it cannot tell the deliveries apart. One row per `seller_id`, newest `ingested_at` first.

Both carry `source_file` and `ingested_at` through to Silver. Beyond lineage they are the only record of which delivery won the dedupe — Bronze holds every one, so joining back cannot say which was kept. Neither belongs in Gold: `dim_product` and `dim_seller` should name their columns rather than `SELECT *`.

## Data quality

Both gates run at the Bronze to Silver boundary. Rules live in [checks/silver_product.yml](../../checks/silver_product.yml) and [checks/silver_seller.yml](../../checks/silver_seller.yml), shipped by the bundle and passed to each notebook as its `checks_path` job parameter.

The requirement's dimension gate is *"unique keys, required names, valid tax IDs, valid product prices, approved currencies"* — which is where these rules come from.

**`silver_product`**

| Rule | Function | Category | Criticality |
|---|---|---|---|
| `product_id_is_null` | `is_not_null` | completeness | error |
| `product_name_is_empty` | `is_not_null_and_not_empty` | completeness | error |
| `product_id_is_not_unique` | `is_unique` | uniqueness | error |
| `list_price_is_not_positive` | `sql_expression` | validity | warn |

No referential integrity rule, and that is not an omission: a product is a root dimension, with no parent and nothing to point at.

`list_price` is a warning rather than an error by the test used in every gate — does the bad value break a calculation, or only a description? `list_price` appears in no Gold computation, since an order carries its own `unit_price` and the marts sum `net_amount`. A catalogue price of zero is wrong and worth recording, but it costs no total, and the product stays joinable.

**`silver_seller`**

| Rule | Function | Category | Criticality |
|---|---|---|---|
| `seller_id_is_null` | `is_not_null` | completeness | error |
| `seller_name_is_empty` | `is_not_null_and_not_empty` | completeness | error |
| `seller_id_is_not_unique` | `is_unique` | uniqueness | error |
| `tax_id_is_not_a_nip` | `regex_match` | validity | warn |
| `currency_code_not_in_bronze_currency` | `foreign_key` | referential integrity | warn |

The tax id check is a **format** check, not a validity proof: ten digits grouped 3-3-2-2, as the invoices print the seller's NIP. A well-formed NIP can still be the wrong one. It is a warning because the seller stays joinable and no amount depends on it.

`silver_dq_quarantine` is a project-wide table shared by every quality gate, told apart by `source_table` — don't drop it when rebuilding. Its shape and the projection into it live in [src/common/dq.py](../common/dq.py), so no gate writes that SQL itself.

The rules are covered by [tests/test_quality_gate.py](../../tests/test_quality_gate.py), which validates every rule file without needing compute.

## Design decisions not specified in the requirements

| Topic | Decision | Reason |
|---|---|---|
| Excel reader | Native `cloudFiles.format = "excel"` | Built into the runtime, so no external package — flow step 5 asks to check first whether Databricks solves a step natively. `spark-excel` was ruled out anyway: serverless supports neither Maven coordinates, JAR libraries in notebooks, nor third-party Spark data sources, and Free Edition has no classic compute to fall back to. |
| Incremental loading | Auto Loader, not a plain read | The file arrival trigger does not pass the arriving file name to the job and its path cannot contain a wildcard, so the job has to work out for itself what is new. Auto Loader's checkpoint does that, which is what makes "a new file is a new delivery" hold for arbitrary file names. A plain read of the folder would re-ingest every past delivery on each run. |
| `.xlsx` filtering | `pathGlobFilter` on the reader | The trigger has no extension filter, so the only available filtering is reader-side. |
| A Silver step for both entities | `silver_product` and `silver_seller` | `INGESTION_PLAN.md` shows both published straight to Gold, but `unit_price` has to become a `DECIMAL` and be renamed somewhere, and `bronze_seller` has to be deduped somewhere. Doing either in Gold would make Gold the only place in the project that casts and dedupes. A Silver step also gives the quality gate the Bronze to Silver boundary the requirement puts it on, which flow 02 otherwise does not have. |
| `silver_seller` transformations | None beyond the dedupe | Its Bronze columns are already `DIM_SELLER`'s, with the types the model wants. |
| Product column types | All `STRING`, nothing cast | The flow says read the file as-is. The sheet's `unit_price` has to become a decimal and `unit_price` → `list_price` has to be renamed for `DIM_PRODUCT`; both happen downstream. |
| Checkpoint location | `_checkpoints/bronze_product_excel` in the volume, outside `data/` | Writing the checkpoint must not look like a new arrival to the trigger watching `data/products_excel/`. The `_excel` suffix names the source format, so the folder is identifiable once other flows add checkpoints of their own. |
| `unit_of_measure` | Not populated | `DIM_PRODUCT` has the column but the sheet has no source for it. |
| Seller read format | Delta, not Parquet | The folder has a transaction log and is at version 1: an overwrite tombstoned the version 0 part file, which is still on disk. The Delta reader returns 1 row; a Parquet read of the same folder returns 2. |
| Seller table type | Managed | Unity Catalog does not allow an external table here: "You can't define a table on any data files or directories within a volume." A real external table needs an external location over cloud storage, which Free Edition does not have. The practice external table in the flow doc is therefore not possible; `SELECT * FROM delta.'<path>'` reads the files in place instead. |
| `source_file` for sellers | The folder name, `sellers_delta` | Part file names are an internal Delta detail and change on every overwrite. |
| Product file location | `data/products_excel/`, its own folder | A file arrival trigger cannot filter by extension and its path cannot hold a wildcard, so a folder is the only way to scope it. Watching the shared `data/` folder instead would start this job on every unrelated upload — harmless, since Auto Loader would find no new workbook and write nothing, but it would fill the run history with no-op runs and spend compute on a quota-limited workspace. Every product delivery is therefore expected to land in this folder. |
| Quarantine status column on Bronze | Not added | [docs/INGESTION_PLAN.md](../../docs/INGESTION_PLAN.md) asks Bronze tables to keep one, but it cannot be written at ingest — the gate has not run yet — and filling it afterwards would make Bronze mutable. It is answerable instead by querying `silver_dq_quarantine`, which records the failing row and its `source_table`. |

## Deploy and run

Product workbooks go in `data/products_excel/`, which is also what the trigger watches. Any `.xlsx` file name works — Auto Loader tracks which ones it has already read:

```
/Volumes/<catalog>/bronze_raw/input_data/data/products_excel/products_excel.xlsx
/Volumes/<catalog>/bronze_raw/input_data/data/sellers_delta/
/Volumes/<catalog>/bronze_raw/input_data/_checkpoints/bronze_product_excel   (created by the job)
```

```bash
databricks bundle deploy -t dev
databricks bundle run load_bronze_product -t dev
databricks bundle run load_bronze_seller -t dev
```

`bronze_currency` must exist before `load_bronze_seller` runs, because the seller gate checks the currency against it. A missing reference table fails the task as a missing table, not as a quality warning, so flow 01's currency load has to have run at least once.

Uploading a workbook to `data/products_excel/` starts `load_bronze_product` on its own. File arrival triggers are checked periodically, so the run starts shortly after the upload rather than immediately. Uploads to the rest of `data/` do not start it.

To re-ingest a delivery that was already processed, delete the checkpoint folder — Auto Loader will not read the same file twice otherwise.

## Excel reader limitations

From the native reader's documentation, worth knowing before a new file is delivered:

- one header row only
- merged cells populate the top-left cell and NULL the rest
- password-protected files and `.xlsm` macros are not supported
- schema evolution is not supported with Auto Loader streaming, which is why the schema is explicit and `cloudFiles.schemaEvolutionMode` is `none`

The current sheet is a clean single-header grid, so none of these apply to it today.
