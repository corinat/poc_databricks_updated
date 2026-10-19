# Flow 03 — Customers

Lands the customer batches into one Bronze table, then flattens the customer hierarchy into Silver.

Requirements: [flows/flows/03_customers.md](../../flows/flows/03_customers.md), plus the general rules in [docs/EXERCISE.md](../../docs/EXERCISE.md), [docs/DATA_MODEL.md](../../docs/DATA_MODEL.md) and [docs/INGESTION_PLAN.md](../../docs/INGESTION_PLAN.md).

## What gets built

| Layer | Object | Type | Source | Write mode | Notebook |
|---|---|---|---|---|---|
| Bronze | `bronze_customer` | table | `data/customer_data/*.csv` | append + `mergeSchema` | `01_load_bronze_customer.py` |
| Silver | `silver_customer` | table | `bronze_customer` | overwrite | `02_build_silver_customer.py` |
| Silver | `silver_dq_quarantine` | table | rows the quality gate rejects | append | `02_build_silver_customer.py` |
| Gold | `dim_customer` | table | `silver_customer` | full overwrite | `03_build_gold_dim_customer.py` |

Bronze objects live in schema `bronze_raw` and Silver in `silver`, in the catalog chosen by the bundle target (`poc_dev`, `poc_test`, `poc`). In the `dev` target, development mode prefixes schema names with `dev_<user>_`.

```mermaid
flowchart LR
    CSV1[customers.csv] --> B1[bronze_customer]
    CSV2[customers_dirty.csv] --> B1
    B1 --> S1[silver_customer<br/>level, hierarchy_path, root_customer_id]
```

## The two batches

They are the same source delivered twice, with different headers and different column order:

| `customers.csv` | `customers_dirty.csv` | normalised to |
|---|---|---|
| `customer_name` | `customer name` | `customer_name` |
| `parent_customer_id` | `parent customer id` | `parent_customer_id` |
| `customer_id` | `customer id` | `customer_id` |
| `country` | `country code` | `country` / `country_code` |

Three of the four differences are whitespace. `country` versus `country code` is a real rename, so normalising whitespace does not reconcile them — `country code` becomes `country_code`, which is a different column from `country`. Both therefore exist in `bronze_customer`, each NULL on the rows of the batch that did not supply it. They are reconciled in Silver, where [flows/flows/03_customers.md:37](../../flows/flows/03_customers.md#L37) puts renaming.

`country_code` is the name [docs/DATA_MODEL.md](../../docs/DATA_MODEL.md) uses for `DIM_CUSTOMER`, so the "dirty" file is closer to the target model than the clean one.

Note also that `customers_dirty.csv` holds three rows — customers 111, 112 and 113 — whose values are identical to the same three rows in `customers.csv`. It is a replay, not an update. The duplicates are resolved by the dedupe on `customer_id` in Silver.

## Job and trigger

Defined in [resources/flow03_customer_job.job.yml](../../resources/flow03_customer_job.job.yml), a notebook task on serverless environment version 6.

| Job | Tasks | Trigger |
|---|---|---|
| `load_bronze_customer` | `load_bronze_customer` → `build_silver_customer` → `build_gold_dim_customer` | file arrival on `data/customer_data/` |

Both steps are tasks of one job: an upload is a single event, and Silver has to be rebuilt whenever Bronze changes. `build_silver_customer` depends on `load_bronze_customer`, so a batch that failed to load does not quietly disappear from Silver — the second task never runs.

`build_gold_dim_customer` depends on Silver for the same reason, so the projection is never taken from a table a failed gate left half-written. `dim_customer` is watched by flow 06's mart job, so publishing it starts a mart rebuild five minutes later.

Development mode normally pauses triggers. The job file sets `pause_status: UNPAUSED` explicitly so the trigger stays active in dev too.

## Approach — Bronze

- One read per file. The notebook lists every `.csv` in `data/customer_data/` and reads each on its own.
- Only batches whose file name is not already among the table's `source_file` values are loaded, so an upload does not re-append earlier deliveries. On the first run the table does not exist and everything loads.
- `header=true` and no `inferSchema`, so column names come from each file's own header row and every column lands as `STRING`.
- Header names are normalised to lower snake_case; values are not touched.
- Adds `source_file` and `ingested_at`.
- Append with `mergeSchema`, so a batch bringing a column the table does not have extends the table rather than failing.
- Each batch loads inside its own `try`. A batch that fails does not hold up the others, and the task then fails naming every batch that did not load. A failed batch is never written, so its name never reaches `source_file` and the next run retries it.

## Approach — Silver

- Cleans and renames to the data model: values trimmed, `customer_id` and `parent_customer_id` cast to `INT`, country upper-cased.
- `country` and `country_code` are reconciled into one `country_code`. The expression is built from the columns actually present in Bronze, because `mergeSchema` means the Bronze column set depends on which batches have arrived.
- Dedupes to one row per `customer_id` with `QUALIFY`, newest `ingested_at` first.
- A recursive CTE walks one generation per step, producing `level`, `hierarchy_path` and `root_customer_id`. It is anchored on the customers that start a tree: those with no parent, plus any whose parent is not in the data, so no row is dropped.

Orphaned customers — a `parent_customer_id` pointing at an id that is not present — are findable without an extra column:

```sql
SELECT * FROM silver_customer
WHERE level = 0 AND parent_customer_id IS NOT NULL
```
- `source_file` and `ingested_at` are carried through the dedupe and both branches of the recursion. Beyond lineage, they are the only record of which batch won the dedupe: Bronze holds every delivery of a `customer_id`, so joining back cannot say which one was kept.
- The rows then go through the quality gate; see [Data quality](#data-quality).
- The schema is declared with `CREATE TABLE IF NOT EXISTS`, then filled with `insertInto` in overwrite mode — every row is replaced, since one changed row shifts the level and path of everything below it. `insertInto` writes into the existing table rather than replacing it, so the declared schema survives; it resolves by position, so the select pins the column order.

The current data gives three levels — holding company, account, branch:

| `customer_id` | `level` | `root_customer_id` | `hierarchy_path` |
|---|---|---|---|
| 201 | 0 | 201 | Northwind Holding |
| 101 | 1 | 201 | Northwind Holding > Ava Stone |
| 111 | 2 | 201 | Northwind Holding > Ava Stone > Ava Stone - Warsaw Branch |

## Approach — Gold

`dim_customer` is a projection: the five columns `DIM_CUSTOMER` declares, in its order, and nothing else. Full overwrite, after Silver, with no quality gate of its own — the project's rules put DQX at the Bronze to Silver boundary.

The three hierarchy columns stop at Silver. `level`, `hierarchy_path` and `root_customer_id` are in `silver_customer` because flow 03 asks for the hierarchy to be flattened once, there; `DIM_CUSTOMER` does not declare them, and `parent_customer_id` is enough for the dimension to express the relationship.

`customer_type` is declared and always NULL. Neither customer file has a source for it. It is declared rather than omitted because Gold is the published contract and DATA_MODEL.md is a deliverable: a table with four columns against a model that declares five means the diagram and the table disagree. NULL makes the gap visible in the data.

## The hierarchy is the one transformation with tests

The recursive walk lives in [src/common/transforms.py](../common/transforms.py) as `customer_hierarchy`, and this notebook calls it. The SQL is unchanged; what changes is that it can be called with a view over frames built in memory, which is what [tests/test_transforms.py](../../tests/test_transforms.py) does.

It is the one piece of Silver worth testing, because its output is unguarded in both directions: no DQX rule mentions `level`, `hierarchy_path` or `root_customer_id`, and none of the three reaches `dim_customer`. A regression would surface in no quarantine row, no mart, no dashboard panel and no `check_marts` failure. Two tests cover it — three generations plus an orphan whose parent is absent, and the dedupe keeping the newest batch.

## Data quality

The gate runs at the Bronze to Silver boundary, on the finished hierarchy. It has to run there rather than earlier: the recursion that produces `level` and `hierarchy_path` is a recursive CTE and so happens in SQL, and checking before it would mean making the valid frame addressable from SQL through a temp view, which [docs/EXERCISE.md](../../docs/EXERCISE.md) rules out.

Rules live in [checks/silver_customer.yml](../../checks/silver_customer.yml), shipped by the bundle and passed to the notebook as the `checks_path` job parameter.

| Rule | Function | Category | Criticality |
|---|---|---|---|
| `customer_id_is_null` | `is_not_null` | completeness | error |
| `customer_name_is_empty` | `is_not_null_and_not_empty` | completeness | error |
| `customer_id_is_not_unique` | `is_unique` | uniqueness | error |
| `parent_customer_id_not_in_silver_customer` | `foreign_key` | referential integrity | warn |

`customer_name_is_empty` uses `trim_strings`, because a name of only spaces is as empty as a `NULL` — which is what the requirement's "required names" is about.

**Orphans are a warning, not an error**, and the reason is stronger than "a dangling reference is not fatal". The check is self-referential: `parent_customer_id` against `customer_id` in the same table. Quarantining an orphan would remove it from Silver while its own children stayed, and those children were not flagged because the check ran before the split — Silver would end up with rows pointing at a parent that is not there. The flow already handles orphans gracefully by making them tree roots, and this rule records them rather than changing that:

```sql
SELECT * FROM silver_customer WHERE level = 0 AND parent_customer_id IS NOT NULL
```

`silver_dq_quarantine` is a project-wide table shared by every quality gate, told apart by `source_table` — don't drop it when rebuilding. Its shape and the projection into it live in [src/common/dq.py](../common/dq.py), so no gate writes that SQL itself.


## Design decisions not specified in the requirements

| Topic | Decision | Reason |
|---|---|---|
| One read per file | Required, not a preference | A single `csv()` read over both files would take the first file's header as the schema and map the second by position. With the column orders differing, country values would load into the name column. |
| Header normalisation | Lower snake_case, formatting only | Delta column names cannot contain spaces, so `customer id` cannot be written as-is. Only the formatting is changed; mapping `country code` to `country` would be a semantic rename, which the flow puts in Silver. |
| Column types | All `STRING` | The flow says to cast IDs in Silver, so Bronze does not cast. It also means `mergeSchema` can never hit a type conflict between batches — it only ever adds columns. |
| Header validation | None | [flows/flows/03_customers.md:24](../../flows/flows/03_customers.md#L24) says not to hardcode a schema. A batch whose id column is named differently still lands, as a new column, and can be mapped in Silver; failing the load would leave the delivery outside the lakehouse until code changed. A key-presence check belongs in flow 06. |
| `saveAsTable`, not `insertInto` | Name-based column resolution | `insertInto` resolves by position, which would misalign batches with different column order. |
| Trigger | File arrival on `data/customer_data/` | Flow 03 does not specify a trigger, and "each batch is a new delivery" is an upload event. A trigger path cannot be filtered, so the batches sit in a folder of their own; pointing it at the shared `data/` folder would start the job on every unrelated upload. |
| File selection | Every `.csv` in the folder | "Each batch is a new delivery", so no file name is hardcoded. The folder holds customer batches only, so requiring a `customers` prefix would reject a batch named differently. |
| Incremental loading | Skip file names already in `source_file` | Without it, a triggered run would re-append every batch in the folder, so each new delivery would multiply what is already in the table. Comparing against the target needs no checkpoint and makes a re-run after a failure load only what is outstanding. |
| Re-delivery under the same name | Not re-ingested | Batches are tracked by file name, so a corrected file uploaded under a name already loaded is skipped. A corrected batch therefore needs a new file name. |
| `customer_type` | Not populated | `DIM_CUSTOMER` has the column but neither file has a source for it. |
| `country` / `country_code` | `coalesce` into `country_code` in Silver | `country_code` is the name `DIM_CUSTOMER` uses. The expression is assembled from the columns Bronze actually has, so Silver still builds when only one batch has been delivered. |
| "Latest" for the dedupe | Newest `ingested_at`, then `source_file` | The newest batch to carry a customer wins. `source_file` breaks ties so the result is deterministic when two batches land in the same run and share a timestamp. |
| Silver schema | Declared, not derived | `silver_customer` is consumed by Gold and the dashboard, so the shape is pinned in DDL. A query that stopped producing these types fails the insert instead of silently changing the table, and `customer_id NOT NULL` turns the "every batch has an id" assumption into something enforced. |
| `hierarchy_path` separator | `' > '` | The flow asks for "the chain of names from root to this customer" without naming a separator. |
| `source_file` + `ingested_at` in Silver | Carried through | They are the only record of which batch won the dedupe, which is otherwise lost on write. Dropped when Gold builds `dim_customer`. |
| Orphan criticality | `warn` | The check is self-referential, so removing an orphan would leave its children pointing at a parent that is no longer there. |
| Orphaned customers | Start a tree of their own | A recursive CTE anchored only on parentless customers would never reach a customer whose `parent_customer_id` points at a customer that is not in the data, silently losing it and everything below it. The anchor also admits those rows, so nothing is lost. They stay identifiable without an extra column: `level = 0` together with a non-null `parent_customer_id` means the parent was missing. Quarantining them belongs to flow 06. |
| `NOT EXISTS` for the orphan test | NULL-safe | `parent_customer_id NOT IN (SELECT customer_id FROM deduped)` evaluates to NULL for every row if any `customer_id` in `deduped` is NULL, which would silently restore the dropping behaviour. |
| Hierarchy cycles | Left to Spark's recursion limit | A cycle in `parent_customer_id` would recurse without end; the recursion level limit stops it and fails the task rather than hanging. |

## Deploy and run

Customer batches go in their own folder, which is also what the trigger watches:

```
/Volumes/<catalog>/bronze_raw/input_data/data/customer_data/customers.csv
/Volumes/<catalog>/bronze_raw/input_data/data/customer_data/customers_dirty.csv
```

```bash
databricks bundle deploy -t dev
databricks bundle run load_bronze_customer -t dev
```

The run executes both tasks: Bronze first, then the Silver rebuild.

Uploading a batch to `data/customer_data/` starts the job on its own. File arrival triggers are checked periodically, so the run starts shortly after the upload rather than immediately.

To re-ingest a batch that was already loaded, either give the file a new name or delete its rows from `bronze_customer`.