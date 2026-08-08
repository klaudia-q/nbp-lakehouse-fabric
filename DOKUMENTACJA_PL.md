# Technical Documentation — NBP Data Pipeline (Bronze / Silver / Gold)

This document describes how each layer of the pipeline is implemented: what each notebook does, the design decisions behind it, and the limitations I am aware of. For a general project overview and setup instructions, see [README.en.md](README.en.md).

## 1. Overview

The pipeline implements a medallion architecture (Bronze → Silver → Gold) on Microsoft Fabric using PySpark and Delta Lake. Data comes from three endpoints of the public NBP (National Bank of Poland) API:

- **Table A** — average exchange rates (`mid`)
- **Table C** — buy/sell rates (`bid`/`ask`)
- **Gold prices** (`cenyzlota`)

The final output is a star schema connected to Power BI via DirectLake (`nbp_raport.pbix`).

| Directory | Content |
|---|---|
| `code-bronze/` | `bronze_nbp_all_sources.py` — ingestion from the NBP API |
| `code-silver/` | `silver_transform.py` — cleansing and standardization |
| `code-gold/` | `gold_star_schema.py` — star schema build |
| `pipeline/`, `silver-layer-errors/` | Screenshots of the Fabric orchestration setup and issues encountered |

---

## 2. Bronze layer — `bronze_nbp_all_sources.py`

This layer fetches raw data from three NBP endpoints for the same date range and writes each to its own Delta table. No business transformation happens here — the JSON is only flattened into a tabular structure.

### How the notebook is built

`SCHEMA_EXCHANGE_RATES` and `SCHEMA_GOLD` are explicitly defined Spark schemas. I chose to declare them rather than rely on inference because Spark cannot determine a column's type when every value in a batch is `None`, raising `CANNOT_DETERMINE_TYPE`. This affects Table A, which never returns `bid`/`ask`, and the gold data, which has no currency fields at all.

`SOURCES` is a configuration dictionary — each source is described by a URL template, a Delta write path, a parser name and a table type. Adding another NBP endpoint therefore requires only a new dictionary entry, not changes to the logic.

`generate_date_ranges()` splits the `DATE_FROM`–`DATE_TO` range into chunks of at most 93 days, which is the NBP API's hard limit per request.

`fetch_nbp(url)` performs the HTTP request. I treat a `404` as "no data in this range" and return an empty list — the API returns it for periods without quotations. Network and JSON parsing errors are caught and logged, returning an empty list rather than aborting the entire job.

`parse_exchange_rates()` and `parse_gold()` flatten the API responses into schema-conformant records. They are separate functions because the response structure for rates and for gold is entirely different.

The main loop iterates over sources and date ranges, then writes the accumulated records to Delta in `overwrite` mode, adding the audit columns `_ingestion_timestamp`, `_source` and `_source_name`. At the end the notebook re-reads each Bronze table and prints its row count as a simple confirmation that the write succeeded.

### Known limitations

Writing in `overwrite` mode means every run replaces the full history with data freshly pulled from the API. This works at the current data volume, but there is no incremental load or CDC mechanism — a longer history or a more frequent schedule will require switching to `MERGE`.

Individual HTTP request errors are swallowed silently. This protects the job from being killed by a transient API outage, but there is no alerting if a date range consistently returns empty data. To be added: counting failed requests and warning once they exceed a threshold.

---

## 3. Silver layer — `silver_transform.py`

This layer cleanses the Bronze data: casting types, validating values, removing duplicates, unifying the schemas of tables A and C, and merging them.

### Transformations

**`silver_exchange_rates_a`** (from `bronze_nbp_table_a`) — dates cast via `to_date` and `mid_rate` to `double`, text normalized (`currency_code` uppercased, `currency_name` through `initcap`), `mid_rate > 0` validation, deduplication on the (`effective_date`, `currency_code`) pair. I add empty `bid_rate`/`ask_rate` columns so the schema matches Table C for the later join.

**`silver_exchange_rates_c`** (from `bronze_nbp_table_c`) — the same pattern, with an additional business rule: `bid_rate < ask_rate`. The spread must be positive, so rows violating this are rejected as erroneous.

**`silver_exchange_rates_merged`** — a `FULL OUTER JOIN` of tables A and C on date and currency code, computing `spread = ask_rate - bid_rate`. A full outer join because the two tables could in principle cover different sets of dates.

In this join the currency name and table number are taken from the `a` alias only. If a row exists solely in Table C, `currency_name` will be `NULL`. In practice NBP publishes the same currencies in both tables on every business day, and the case did not occur in the data I have. The direction of the fix remains open — see section 5.

**`silver_gold_prices`** (from `bronze_nbp_gold`) — type casting, `gold_price_pln > 0` validation, deduplication by date.

The notebook ends by printing row counts for each Silver table.

### Diagnostic code

The final cells (`In[5]`, `In[9]`, `In[13]`, `In[16]`) contain the code I used to diagnose a table-visibility problem in the Lakehouse; the screenshots in `silver-layer-errors/` document that debugging session. The code inspects the current Spark catalog and the list of registered tables, then registers the Bronze tables as catalog tables via `CREATE TABLE ... USING DELTA LOCATION` — first with the relative `Tables/...` path and, after that failed, with the full `abfss://` path.

The base path is not hardcoded — it is resolved by the `resolve_base_path()` function. It first checks the `NBP_LAKEHOUSE_TABLES_PATH` environment variable (useful when running from a pipeline) and, failing that, reads the path from `notebookutils.fs.ls("Tables")` and strips the table name. The notebook therefore runs in any workspace without code edits, and no environment identifiers end up in the repository.

---

## 4. Gold layer — `gold_star_schema.py`

This layer builds a star schema ready to be connected in Power BI through DirectLake.

### Output tables

**`gold_dim_date`** — a date dimension built from the union of distinct dates in `silver_exchange_rates_merged` and `silver_gold_prices`. It holds `date_key` as a `yyyyMMdd` integer, plus year, quarter, month and month name, week of year, day of month and of week, day name, and a weekend flag.

**`gold_dim_currency`** — a currency dimension with the surrogate key `currency_key` assigned by `row_number()` ordered by currency code, plus an `is_active` flag. The flag is always `True` because I did not implement logic for deactivating currencies withdrawn from circulation.

The key is reassigned from scratch on every run. Under the current full `overwrite` this stays consistent, because the fact tables are recomputed in the same pass. It will stop being consistent once loading becomes incremental — a new currency appearing mid-alphabet shifts the numbering, and previously written facts start pointing at a different dimension row. The options I am weighing are in section 5.

**`gold_fact_exchange_rate`** — the exchange rate fact table, joined to the dimensions through `currency_key` and `date_key`. Besides `mid_rate`, `bid_rate`, `ask_rate` and `spread`, it holds the daily rate change (`daily_change`, `daily_change_pct`) computed with a `lag()` window function partitioned by currency.

**`gold_fact_gold_price`** — the equivalent table for gold prices, without partitioning, since it is a single time series.

Finally I run `OPTIMIZE ... ZORDER BY` on both fact tables, by `date_key` and `currency_key`. This arranges the physical Delta file layout around the filters Power BI issues most often.

### Implementation notes

`dim_currency` inherits the `currency_name` behaviour described above — the dimension is built from the merged table, so any `NULL`s propagate.

`daily_change_pct` divides by the previous rate or price. Were that value `0` or `NULL`, the result is `NULL` — Spark does not raise a divide-by-zero exception, so I did not add a separate guard.

The opening cells (`In[1]`, `In[2]`) check Silver table visibility and register them in the catalog — the path is resolved by the same `resolve_base_path()` function used in the Silver layer.

---

## 5. Planned for later stages

The project is to be migrated to Databricks, and I plan the following changes along the way:

- replacing the full reload with incremental loading (`MERGE` instead of `overwrite`),
- **deterministic deduplication** — I currently use `dropDuplicates(["effective_date", "currency_code"])`, which keeps an arbitrary row when two share the same key. To be replaced with `row_number()` ordered explicitly by `_ingestion_timestamp DESC` so the most recent write always wins,
- **retry with exponential backoff** on API calls — a single HTTP error currently yields an empty list for the whole date range with no second attempt; alongside that, counting failed requests and warning once they exceed a threshold,
- moving the date range into external configuration rather than in-code values,
- unifying the Bronze write path — currently I write by path (`.save`) but read by table name in later layers, which forces the manual `CREATE TABLE ... LOCATION` registration,
- unit tests for the parsing functions and a `requirements.txt` file.

Two questions I am deliberately leaving open, since both surface only once loading becomes incremental and both have arguments on several sides:

- **the surrogate key in `dim_currency`** — a persistent mapping via `MERGE` (keys assigned only to new codes), a deterministic key derived from `sha2(currency_code, 256)` (no state to maintain, but unreadable and wider than an integer), or dropping the surrogate in favour of `currency_code` as a natural key (the dimension is small and carries no history),
- **the join type between tables A and C** — keep the `FULL OUTER JOIN` and add `coalesce()`, or drop to an `INNER JOIN` and log unpaired rows as an anomaly.
