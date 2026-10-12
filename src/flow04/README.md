# Flow 04 — Invoices

Parses invoice PDFs into a Bronze `VARIANT` table, then flattens the headers into Silver.

Requirements: [flows/flows/04_invoices.md](../../flows/flows/04_invoices.md), plus the general rules in [docs/EXERCISE.md](../../docs/EXERCISE.md), [docs/DATA_MODEL.md](../../docs/DATA_MODEL.md) and [docs/INGESTION_PLAN.md](../../docs/INGESTION_PLAN.md).

## What gets built

| Layer | Object | Type | Source | Write mode | Notebook |
|---|---|---|---|---|---|
| Bronze | `bronze_invoice` | table | `data/invoices/*.pdf` | append | `01_list_invoice_files.py`, `02_parse_invoice_file.py` |
| Silver | `silver_invoice` | table | `bronze_invoice` | overwrite | `03_build_silver_invoice.py` |
| Silver | `silver_dq_quarantine` | table | rows the quality gate rejects | append | `03_build_silver_invoice.py` |

Bronze objects live in schema `bronze_raw` and Silver in `silver`, in the catalog chosen by the bundle target (`poc_dev`, `poc_test`, `poc`). In the `dev` target, development mode prefixes schema names with `dev_<user>_`.

```mermaid
flowchart LR
    PDF[invoices/*.pdf] --> L[list_invoice_files]
    L -->|task value: file list| FE[for_each_invoice]
    FE --> P[parse_invoice_file<br/>one run per file]
    P --> B1[bronze_invoice<br/>parsed_data VARIANT]
    B1 --> DQ{DQX}
    DQ -->|passed| S1[silver_invoice<br/>header columns]
    DQ -->|flagged| Q[silver_dq_quarantine]
```

## Job and triggers

Defined in [resources/flow04_invoice_job.job.yml](../../resources/flow04_invoice_job.job.yml). Notebook tasks only, on serverless environment version 6.

| Task | Type | Purpose |
|---|---|---|
| `list_invoice_files` | Notebook | Publishes the PDFs still to parse as a task value |
| `for_each_invoice` | For Each | One parse run per file, concurrency 4 |
| `build_silver_invoice` | Notebook | Rebuilds the Silver header table |

Trigger: file arrival on `data/invoices/`, with `wait_after_last_change_seconds: 60` so a bulk upload settles into one run. Development mode normally pauses triggers; the job sets `pause_status: UNPAUSED` so it stays active in every target.

`max_concurrent_runs: 1`. Step 1 decides what to parse by reading `source_file` from `bronze_invoice`, and those rows appear only as each For Each iteration commits, so runs have to be serialised for that list to be accurate.

## The source PDFs

Thirty Polish VAT invoices, around 4 KB each, generated as text — uncompressed content streams, Helvetica, no images — so no OCR is involved. Fields are printed under Polish labels: `Faktura VAT {invoice_id} oryginal`, `Order ID`, `Miejsce wystawienia`, `Kraj`, `NIP`, `Razem do zaplaty`.

The invoices cross-reference the other flows: the seller is `Firma Testowa DataLake Sp. z o.o.`, NIP `111-111-11-11`, which is `SEL-001` in `sellers_delta`, and the line items are products from the Excel catalogue.

Amounts are printed with a decimal comma, and the gross total is printed next to its currency (`60,27 GBP`).

## Approach — Bronze

### Step 1 — `list_invoice_files`

- Lists the PDFs with `dbutils.fs`, so no package is installed.
- Drops the files whose path is already among `bronze_invoice`'s `source_file` values. Parsing one invoice takes about 20 seconds of AI function time, so a run only pays for invoices it has not seen.
- Publishes the remaining paths as the task value `invoice_files`, which is the For Each task's input.
- `dbutils.fs.ls` returns volume paths with a `dbfs:` prefix; it is stripped so `source_file` holds the plain `/Volumes` path, which is also the value Silver carries as `pdf_path`.

### Step 2 — `parse_invoice_file`

Runs once per file, as the nested task of the For Each.

- `ai_parse_document` turns the PDF into a structured document and `ai_extract` pulls the fields out of it. Both are native to the runtime, so no PDF or LLM package is installed.
- The field definitions come from [docs/invoice_extraction_schema.json](../../docs/invoice_extraction_schema.json), which is already in `ai_extract`'s advanced schema format — types, descriptions, the `invoice_status` enum, and the nested `buyer`, `seller`, `totals` and `items` structures. The descriptions carry the Polish labels the invoices print, which is what lets the fields be found.
- The schema is read from the deployed bundle through the `schema_path` job parameter, built from `${workspace.file_path}`, which resolves per target. It is parsed and re-serialised, so a malformed schema fails with a JSON parse error naming the position.
- Both function versions are pinned, `2.0` for the parse and `2.1` for the extract.
- The whole `ai_extract` result is stored, `error_message` and `metadata` included, so a partial extraction is visible in Bronze.
- `VARIANT` because the `items` array is nested by tax level and then by item; Bronze keeps that shape without modelling it.
- Append, one row per file.

## Approach — Silver

- One row per invoice. The `items` array stays in the Bronze `VARIANT`: this is the header table, and the line items do not flatten into one row per invoice.
- Columns are renamed to the data model — `totals.total_net_amount` becomes `net_amount`, `totals.currency` becomes `currency_code`.
- Amounts are normalised before casting: anything that is not a digit, comma, dot or minus is dropped, then the comma becomes a decimal point. This handles both `49,00` and `60,27 GBP`, and leaves a clean number untouched.
- Dates and amounts use `try_cast`, so a value the extraction got wrong becomes NULL rather than failing the query, and is caught by the quality gate instead.
- Deduplicates to one row per `invoice_id`, newest `ingested_at` first. The partition is `coalesce(invoice_id, pdf_path)`, not `invoice_id` alone: an extraction that lost the id leaves it NULL, and every such row would fall in one partition with all but one dropped — so several failed extractions would be reported as one. `pdf_path` identifies the row whatever else is missing.
- The rows then go through the quality gate; see [Data quality](#data-quality).
- The schema is declared with `CREATE TABLE IF NOT EXISTS` and filled with `insertInto` in overwrite mode, which writes into the existing table rather than replacing it, so the declared schema and the `CLUSTER BY (invoice_id)` survive. It resolves by position, so the select pins the column order.

## Data quality

The gate runs at the Bronze to Silver boundary. Rules live in [checks/silver_invoice.yml](../../checks/silver_invoice.yml), shipped by the bundle and passed to the notebook as the `checks_path` job parameter.

The fields are extracted by an LLM, so any of them can come back missing. That is what makes completeness the substance of this gate rather than a formality.

| Rule | Function | Category | Criticality |
|---|---|---|---|
| `invoice_id_is_null` | `is_not_null` | completeness | error |
| `invoice_date_is_null` | `is_not_null` | completeness | error |
| `net_amount_is_null` | `is_not_null` | completeness | error |
| `vat_amount_is_null` | `is_not_null` | completeness | error |
| `gross_amount_is_null` | `is_not_null` | completeness | error |
| `invoice_id_is_not_unique` | `is_unique` | uniqueness | error |
| `net_plus_vat_is_not_gross` | `sql_expression` | validity | error |
| `order_id_not_in_silver_order` | `foreign_key` | referential integrity | warn |

An `error` row goes to `silver_dq_quarantine` and never reaches Silver. A `warn` row goes to both — an invoice billing an order that has not loaded yet is still a real invoice, and losing it would understate revenue.

`net_plus_vat_is_not_gross` is the requirement's own arithmetic. All three amounts are `DECIMAL(18,2)`, so the comparison is exact rather than approximate. A row with a missing amount makes the expression NULL and is not flagged here; the completeness rules catch it instead.

**One consequence to know:** `silver_invoice` can be incomplete. One bad extraction out of thirty means twenty-nine invoices are published and one is quarantined, so a total summed over the table is wrong without looking wrong. The gap is visible rather than silent — it shows up as a quarantine row, which is what flow 07's quarantine alert watches.

Not in the gate, deliberately: "matching seller/buyer" from the requirement's invoice gate. The PDFs carry the seller's name and tax id but no `seller_id`, so checking it means resolving against `dim_seller` — a business join that belongs in Gold.

`silver_dq_quarantine` is a project-wide table shared by every quality gate, told apart by `source_table` — don't drop it when rebuilding. Its shape and the projection into it live in [src/common/dq.py](../common/dq.py), so no gate writes that SQL itself.

The rules are covered by [tests/test_quality_gate.py](../../tests/test_quality_gate.py), which validates every rule file without needing compute.

## Design decisions not specified in the requirements

| Topic | Decision | Reason |
|---|---|---|
| PDF parsing and extraction | `ai_parse_document` and `ai_extract` | The flow asks twice to check for a native option, and these are native to the runtime. The extraction schema file is already in `ai_extract`'s advanced format, so it is passed to the function directly. |
| Function versions | Pinned, `2.0` and `2.1` | A change in defaults cannot alter what lands in Bronze. |
| What is stored in `VARIANT` | The whole `ai_extract` result | `error_message` and `metadata` stay available, so a partial extraction is visible in Bronze. |
| Schema file location | Read from `${workspace.file_path}` | The schema is configuration that belongs in the repo next to the notebook, so it deploys with the code and the two versions cannot drift. The substitution resolves per target. |
| For Each concurrency | 4 | Each invoice spends about 20 seconds waiting on AI functions. Low enough to stay clear of rate limits. |
| Trigger | File arrival on `data/invoices/` | An upload is the event that produces new invoices, and the folder already holds invoice PDFs only. |
| Debounce | 60 seconds | A bulk upload settles into one run, and each run is a startup plus a scan of `bronze_invoice`. |
| Amount normalisation | Strip non-numeric, comma to decimal point | The invoices print amounts with a decimal comma, and the gross total with its currency attached. |
| Missing fields | Task fails before the write | These are money columns, and a NULL amount reaching a `SUM` downstream produces a total that is wrong but plausible. Naming the PDF and the fields makes the failure actionable. |
| `invoice_date` and amounts | `try_cast` | The extraction is an LLM call, so a field can come back in an unexpected form. The value becomes NULL and the check reports which file and field it was. |
| Silver clustering | `CLUSTER BY (invoice_id)` | `invoice_id` is unique, which liquid clustering handles, and clustering is table metadata that persists across the overwrite. |
| Silver schema | Declared, not derived | `silver_invoice` is consumed by Gold and the dashboard, so the shape is pinned in DDL, with `invoice_id NOT NULL`. |
| `seller_id` | Not populated | `FACT_INVOICE` has the column, and the PDFs carry the seller's name, address and tax id but no id. Resolving it against `dim_seller` on `tax_id` is a business join and belongs in Gold. |
| Line items | Left in the Bronze `VARIANT` | The flow specifies a header table. The items are nested two levels deep, by tax level and then by item. |

## Deploy and run

Invoice PDFs go in `data/invoices/`, which is also what the trigger watches:

```
/Volumes/<catalog>/bronze_raw/input_data/data/invoices/*.pdf
```

```bash
databricks bundle deploy -t dev
databricks bundle run load_bronze_invoice -t dev
```

The run executes all three steps: list, the parse fan-out, then the Silver rebuild. Uploading PDFs to `data/invoices/` starts the job on its own; file arrival triggers are checked periodically, so the run begins shortly after the upload.

To re-parse an invoice, delete its row from `bronze_invoice` — step 1 skips files whose path is already recorded there.

## Not verified

Nothing has been run end to end. Four things to confirm on the first run:

- Whether the full extraction schema finds `order_id` and `invoice_status`. A call using bare field names returned both as null; the descriptions in the schema file name the printed labels, which is what should fix it.
- Whether the amounts arrive as numbers now that the schema declares them as such. The normalisation handles either form.
- The `VARIANT` paths in Silver assume scalar leaves are wrapped in `value` and containers are not, so `parsed_data:response:buyer:name:value`. The two helper functions at the top of the notebook are the only place this is expressed.
- Whether a For Each over an empty list runs zero iterations. This happens on any run where every invoice is already parsed.

## Later

[docs/INGESTION_PLAN.md](../../docs/INGESTION_PLAN.md) assigns the invoice data quality checks to flow 06: unique invoice id, valid order id, matching seller and buyer, and `net + VAT = gross`. The missing-field check in step 3 covers the money columns until then.
