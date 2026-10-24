# ConductLens architecture and contracts

This file is the contract between the three code areas (`ingestion/`, `dbt/`, `airflow/`).
Anything that crosses a boundary (table names, columns, env vars, run modes) is defined here
and nowhere else. The README explains *why*; this file says *what*.

## 1. Shape of the system

```
                 daily bulk CSV (5.6 GB, 18.7M rows)      FDIC BankFind API        FRED fredgraph.csv
                 files.consumerfinance.gov/ccdb/          api.fdic.gov/banks       fred.stlouisfed.org
                           │                                     │                        │
                           ▼                                     ▼                        ▼
                  ┌──────────────────────── ingestion (Python, runs in Airflow) ───────────────────────┐
                  │ download → normalise → parquet landing (CL_DATA_DIR) → row_hash → window filter   │
                  └──────────────┬──────────────────────────────────────────────┬──────────────────────┘
                                 │ load-job append of rows whose                │ MERGE … WHEN NOT MATCHED
                                 │ (id,row_hash) is not already there           │ on (id,row_hash)
                                 ▼                                              ▼
                 BigQuery sandbox  <project>.conductlens_raw.*        Databricks Free  workspace.conductlens_raw.*
                 (brief's warehouse; expires every 60 d,              (durable replica; MERGE allowed,
                  no DML, 10 GiB lifetime writes)                      nothing expires)
                                 │                                              │
                                 ▼  dbt --target bq                             ▼  dbt --target dbx
                 staging = views, marts = small tables                staging/intermediate = tables,
                 no incremental, no snapshot                          marts = tables, snapshot, tests
                                 │                                              │
                                 └───────── parity check (Airflow task) ────────┘
                                                                                │
                                                                                ▼
                                                                  Hex (Community) reads Databricks marts
```

Both warehouses hold the **same raw logical content** and run the **same dbt project**. Hex is bound
to Databricks because sandbox tables die after 60 days; if BigQuery is ever rebound, no query
changes because mart names and columns are identical by construction.

## 2. Naming

| Layer | BigQuery (project = `CL_BQ_PROJECT`) | Databricks (catalog = `CL_DBX_CATALOG`) |
|---|---|---|
| raw | dataset `conductlens_raw` | schema `conductlens_raw` (+ volume `landing`) |
| staging + intermediate | dataset `conductlens_staging` | schema `conductlens_staging` |
| marts | dataset `conductlens_marts` | schema `conductlens_marts` |
| snapshots | (not run: needs MERGE) | schema `conductlens_snapshots` |

Prefix comes from `CL_BQ_DATASET_PREFIX` / `CL_DBX_SCHEMA_PREFIX` (default `conductlens`). dbt's
`generate_schema_name` macro returns `<prefix>_<folder schema>` verbatim, never `<target>_<schema>`.

dbt profile name: `conductlens`; targets `bq` and `dbx`; default target `dbx`. `profiles.yml` lives
in `dbt/` and reads only env vars (see §7).

## 3. Raw tables (created and written by ingestion only; dbt treats them as sources)

All raw tables are **append-only version logs**. A row is never updated or deleted. The business
key plus `row_hash` is unique; staging picks the newest `_loaded_at` per business key. Re-running
any window re-derives the same hashes, finds them present, and writes nothing: that is the
idempotency guarantee on both warehouses.

Audit columns on every raw table:

| column | type | meaning |
|---|---|---|
| `row_hash` | STRING(32) | first 32 hex chars of sha256 over the business columns, in the fixed order listed per table, values stringified, nulls as empty string, joined by `\x1f` |
| `_source` | STRING | `bulk_csv`, `api`, `fdic_api`, `fred_csv` |
| `_source_as_of` | DATE | for CFPB: the bulk file's `Last-Modified` date (UTC); FDIC: run date; FRED: run date |
| `_loaded_at` | TIMESTAMP (UTC) | when this row was written |
| `_run_id` | STRING | Airflow run id or `local-<utc timestamp>` |
| `_is_tombstone` | BOOL | `true` marks "this key disappeared from the source" (full-diff mode only); business columns are null on tombstones and `row_hash = 'tombstone'` |

### 3.1 `conductlens_raw.cfpb_complaints` (Databricks; BigQuery only when `CL_BQ_CFPB_GRAIN=complaints`)

Business key `complaint_id`. Hash order: `received_date, product, sub_product, issue, sub_issue,
company_public_response, company, state, zip_code, tags, submitted_via, date_sent_to_company,
company_response, timely_response, consumer_disputed, consumer_consent_provided, has_narrative`.

| column | type | notes |
|---|---|---|
| `complaint_id` | INT64 | |
| `received_date` | DATE | CFPB calendar date, US Eastern, no time. Never converted. |
| `received_month_id` | INT64 | `year*100+month`, e.g. `202610`. BigQuery integer-range partition key (201101–203612, interval 1). |
| `product`, `sub_product`, `issue`, `sub_issue` | STRING | verbatim CFPB text, nulls kept as null |
| `company` | STRING | verbatim free text, e.g. `JPMORGAN CHASE & CO.` |
| `company_public_response` | STRING | |
| `state` | STRING | 2-letter or null |
| `zip_code` | STRING | may be `326XX` |
| `tags` | STRING | `Servicemember`, `Older American`, both, or null |
| `submitted_via` | STRING | |
| `date_sent_to_company` | DATE | nullable |
| `company_response` | STRING | `Closed with explanation`, … , `In progress`, `Untimely response` |
| `timely_response` | STRING | `Yes` / `No` / null (staging casts to BOOL) |
| `consumer_disputed` | STRING | **only from the API source**; the bulk CSV no longer carries it and the CFPB stopped collecting it on 2017-04-24. Null otherwise. |
| `consumer_consent_provided` | STRING | API only; null otherwise |
| `has_narrative` | BOOL | API only. The narrative **text is never landed**. |

Databricks: Delta, `CLUSTER BY (received_date, company)`. Size reference (2026-10-24 bulk file):
18,653,525 complaints, 2011-12-01 … 2026-10-23, 8,189 distinct company strings, 287 bytes/row in CSV;
**7.7 GB of BigQuery logical bytes** if landed as-is, which is why BigQuery gets §3.1b instead.

### 3.1b `conductlens_raw.cfpb_complaints_daily` + `cfpb_codes` (BigQuery default: `CL_BQ_CFPB_GRAIN=daily_cells`)

The sandbox grants a **lifetime** 10 GiB of writes, expires every table after 60 days and has no
DML (verified on docs.cloud.google.com/bigquery/docs/sandbox, page dated 2026-10-22). Keeping
18.7M complaint rows alive there would cost ~7.7 GB per 60-day rotation. So BigQuery lands the
same log at the **daily cell** grain every business question needs, with strings replaced by
integer codes: ~5.2M cells, ~0.7 GB per rotation, ~3 MB/day of new cell versions.

`cfpb_complaints_daily` — business key = all `*_code` columns + `received_date`; `row_hash` =
first 16 hex chars of sha256 over `n_complaints` (the only non-key column), so a cell whose
count changes gets a new version and a cell that disappears gets a tombstone (`_is_tombstone`).
Same dedupe-latest rule as every other log. `_run_id` is omitted on this table to save bytes;
`_loaded_at` joins to `_load_runs.started_at..finished_at`.

| column | type | notes |
|---|---|---|
| `received_date` | DATE | |
| `received_month_id` | INT64 | partition key (integer range) |
| `company_code`, `product_code`, `sub_product_code`, `issue_code`, `state_code`, `company_response_code`, `timely_response_code`, `consumer_disputed_code` | INT64 | **0 means null**; otherwise joins `cfpb_codes` on (`dimension`, `code`) with `dimension` = the column name without `_code` |
| `n_complaints` | INT64 | count of complaints in the cell |
| `row_hash` STRING(16), `_source`, `_source_as_of`, `_loaded_at`, `_is_tombstone` | | audit (no `_run_id`) |

Clustered by `company_code, product_code`.

`cfpb_codes` — append-only dictionary: `dimension` STRING, `code` INT64, `value` STRING,
`_loaded_at`, `_run_id`. Codes are assigned by ingestion (`max(code)+1` per dimension for
unseen values) and never reused or changed. ~9k rows.

dbt on BigQuery therefore has `stg_cfpb_complaints_daily` (decode + latest version per cell) and
no `stg_cfpb_complaints`; on Databricks it is the reverse. Both targets expose the same
`int_complaints_daily` (grain: `received_date, company, product, sub_product, issue, state,
company_response, timely_response, consumer_disputed` → `n_complaints` plus enrichments), and
**every mart reads only `int_complaints_daily`**, so mart SQL is identical on both warehouses.
What BigQuery loses versus Databricks: `zip_code`, `tags`, `submitted_via`, `sub_issue`,
`date_sent_to_company`, `company_public_response` (none is needed by the ten questions).

### 3.2 `conductlens_raw.fdic_institutions`

Business key `cert`. Pulled in full every run (active and inactive, ~27k rows), from
`GET https://api.fdic.gov/banks/institutions?fields=…&limit=10000&offset=…`. Hash order is the
column order below (excluding audit columns).

| column | type | FDIC field |
|---|---|---|
| `cert` | INT64 | CERT |
| `name` | STRING | NAME |
| `city` | STRING | CITY |
| `stalp` | STRING | STALP |
| `bkclass` | STRING | BKCLASS |
| `active` | BOOL | ACTIVE (1/0) |
| `fed_rssd` | STRING | FED_RSSD |
| `rssdhcr` | STRING | RSSDHCR (holding-company RSSD id; empty when none) |
| `namehcr` | STRING | NAMEHCR |
| `asset_thousands` | INT64 | ASSET (USD thousands) |
| `dep_thousands` | INT64 | DEP (USD thousands, total deposits) |
| `offices` | INT64 | OFFICES |
| `repdte` | DATE | REPDTE (financials report date, `MM/DD/YYYY` in the API) |
| `dateupdt` | DATE | DATEUPDT |
| `estymd` | DATE | ESTYMD (established) |
| `endefymd` | DATE | ENDEFYMD (end of existence, inactive only) |
| `webaddr` | STRING | WEBADDR |

### 3.3 `conductlens_raw.fred_observations`

Business key `(series_id, observation_date)`. Source: `https://fred.stlouisfed.org/graph/fredgraph.csv?id=<SERIES>&cosd=<start>` (no API key). `.` values become null. Hash order: `series_id, observation_date, value`.

| column | type |
|---|---|
| `series_id` | STRING |
| `observation_date` | DATE |
| `value` | FLOAT64 |

Default series (`CL_FRED_SERIES`): `DGS10` (10y Treasury, daily), `DFF` (effective fed funds, daily),
`DPRIME` (bank prime, daily), `MORTGAGE30US` (30y fixed, weekly Thursday), `TERMCBCCALLNS`
(commercial bank credit-card rate, quarterly). Weekly/quarterly series are forward-filled to daily
in dbt (`int_rates_daily`), never in ingestion.

### 3.4 `conductlens_raw._load_runs`

One row per (run, source, sink). Written on success and on failure.

| column | type |
|---|---|
| `run_id` STRING | `source` STRING (`cfpb`,`fdic`,`fred`) | `sink` STRING (`bigquery`,`databricks`) |
| `mode` STRING | `incremental`, `backfill`, `full_diff`, `rehydrate` |
| `window_start` DATE, `window_end` DATE | received/observation date window that was processed (null for FDIC) |
| `rows_read` INT64 | rows in the window from the source |
| `rows_written` INT64 | rows appended (new versions + tombstones) |
| `status` STRING | `success` / `failed` |
| `error` STRING | truncated to 2000 chars |
| `started_at`, `finished_at` TIMESTAMP | UTC |

### 3.5 `conductlens_raw._load_state`

Watermark per (source, sink): `source`, `sink`, `watermark_date` DATE, `source_as_of` DATE,
`last_run_id`, `updated_at`. For CFPB `watermark_date` = max `received_date` fully loaded; the next
incremental window is `[watermark_date - CL_CFPB_OVERLAP_DAYS, today]`. It is only ever raised.
On BigQuery `_load_state` is rewritten with a `WRITE_TRUNCATE` load job (tiny table; DML is not available).

## 4. Ingestion run modes (`cl-ingest`)

```
cl-ingest run  --source cfpb|fdic|fred|all  [--start YYYY-MM-DD --end YYYY-MM-DD]
               [--mode incremental|backfill|full_diff] [--sinks bigquery,databricks] [--run-id X]
cl-ingest ensure-tables [--sinks …]          # creates datasets/schemas/volume/tables, rotates expiring BQ tables
cl-ingest rehydrate --sink bigquery          # full load from the latest landed parquet after a sandbox expiry
cl-ingest status                             # prints _load_state and last 10 _load_runs per sink
```

* `incremental` (default): window from watermark minus overlap to today.
* `backfill`: explicit `--start/--end`; raises the watermark only if `end > watermark`.
* `full_diff` (CFPB only, weekly in Airflow): every row in the file is hashed and compared; keys
  missing from the file get tombstones. Catches mutations older than the 60-day window.
* `rehydrate` (BigQuery only): when `ensure-tables` finds `cfpb_complaints` missing or expiring in
  <5 days, it recreates the table and loads the full latest parquet. Written to `_load_runs` as
  `mode='rehydrate'`.

Landing zone layout (`CL_DATA_DIR`): `cfpb/complaints_<as_of>.csv.zip`, `cfpb/complaints_<as_of>.parquet`,
`fdic/institutions_<run_date>.parquet`, `fred/<series>_<run_date>.parquet`. Keep the last 3 of each.

## 5. dbt project `conductlens` (same models on both targets)

Materialisation rule, implemented in one macro `cl_materialized(default)`:

| folder | Databricks | BigQuery (sandbox) |
|---|---|---|
| staging | view | view |
| intermediate | table | **view** (no incremental: no DML; no table: every write counts against 10 GiB lifetime) |
| marts | table | table (small aggregates only; keep the BigQuery marts under ~3 MB/day in total: TTM or trailing-36-month windows, companies above a volume floor) |
| snapshots | run | `enabled: false` |

Sources: `raw.cfpb_complaints` (Databricks), `raw.cfpb_complaints_daily` + `raw.cfpb_codes` (BigQuery), `raw.fdic_institutions`, `raw.fred_observations`, `raw._load_runs`
with `loaded_at_field: _loaded_at` freshness (warn 36 h, error 72 h).

### Staging (one per source, dedupe to current version)
* `stg_cfpb_complaints` (Databricks only, `enabled: target.type == 'databricks'`): latest version per `complaint_id`, tombstones removed, `timely_response`→`is_timely` BOOL,
  `response_category` ∈ {`explanation`,`monetary_relief`,`non_monetary_relief`,`in_progress`,`closed_other`,`untimely`},
  `days_to_forward = date_sent_to_company - received_date`, `company_name_norm` via `normalize_company_name()`.
* `stg_cfpb_complaints_daily` (BigQuery only): latest version per cell, tombstones removed, codes decoded through `cfpb_codes` (code 0 → null), same derived columns as above at cell grain plus `n_complaints`.
* `stg_fdic_institutions`: latest per `cert`; deposits/assets in USD (×1000); `holding_key = coalesce(rssdhcr, 'CERT-'||cert)`.
* `stg_fred_observations`: latest per (series, date), nulls dropped.

### Intermediate
* `int_product_groups`: product/sub-product → `product_group` ∈ {`credit_reporting`,`debt_collection`,`mortgage`,`credit_card`,`checking_savings`,`money_transfer`,`crypto`,`bnpl`,`student_loan`,`auto_loan`,`personal_loan`,`prepaid`,`other`} from seed `product_groups.csv`; unmatched → `other` (test warns on unmatched volume).
* `int_company_mapping`: CFPB `company` → `holding_key`, `match_method` ∈ {`seed`,`norm_name_holding`,`norm_name_charter`,`unmatched`}, `is_digital_first`, `digital_segment`. Seed `company_fdic_map.csv` wins; then normalised-name join on `namehcr`, then on `name`.
* `int_complaints_daily`: **the one model every mart reads.** Grain `received_date, company, product, sub_product, issue, state, company_response, timely_response, consumer_disputed` with `n_complaints`; on Databricks a `group by` over `stg_cfpb_complaints`, on BigQuery a pass-through of `stg_cfpb_complaints_daily`. Enriched with product group, mapping (`holding_key`, `match_method`), `is_digital_first`, `response_category`, `is_timely`, `received_week` (Monday), `received_month`, `received_year`.
* `int_fdic_deposits_by_holding`: sum of `dep_usd`, `asset_usd` per `holding_key`, `n_charters`, `size_bucket` ∈ {`<1B`,`1-10B`,`10-50B`,`50-250B`,`>250B`}.
* `int_rates_daily`: date spine 2011-12-01→today × series, forward-filled values.
* `int_complaints_weekly`: week × company × product_group × issue counts (for q8, q9).

### Marts (one per business question, plus dims)
| model | grain | answers |
|---|---|---|
| `dim_company` | company | mapping, flags, deposits, size bucket |
| `mart_volume_periods` | period_type × period | q1: WTD/MTD/YTD vs same period last year, with `pct_change` |
| `mart_product_trend_monthly` | month × product_group | q2: count, share, yoy growth |
| `mart_issue_trend_monthly` | month × product_group × issue | q2: fastest-growing issues (TTM vs prior TTM) |
| `mart_company_timeliness_monthly` | month × company | q3: complaints, `timely_share`, `ttm_timely_share` (window), 15-day flag basis documented |
| `mart_company_resolution` | company × response_category (TTM) | q4 |
| `mart_dispute_rate` | company × year | q5: `dispute_rate`, `coverage_share` (share of rows with a non-null disputed flag) |
| `mart_company_benchmark` | company (TTM) | q6: `complaints_ttm`, `deposits_bn`, `complaints_per_bn_deposits`, `size_bucket`, `rank_in_bucket`, `peer_group` ∈ {`chartered`,`fintech_unnormalised`}, `is_our_company` |
| `mart_digital_vs_chartered` | month × segment | q7: timely share, dispute rate, response mix |
| `mart_weekly_spikes` | week × company × issue | q8: `this_week`, `trailing_8w_avg`, `trailing_8w_stddev`, `z_score`, `is_spike` (z>2 and avg≥5) |
| `mart_rates_weekly` | week × series | q9 series for charts, plus weekly mortgage/credit-card counts |
| `mart_rates_lag_correlation` | product_group × series × lag_weeks (0–26) | q9: `corr_levels`, `corr_changes` (4-week log-change vs lagged rate change), `best_lag` flag |
| `mart_state_complaints` | state × product_group (TTM) | q10: count, `per_100k` (seed population), `region`, product mix share |
| `mart_company_match_coverage` | match_method | honesty table: share of complaints and of bank-product complaints per match method |

Snapshot: `snap_fdic_institutions` (`check` strategy on name, namehcr, rssdhcr, active, dep_thousands, asset_thousands), Databricks only.

Seeds: `company_fdic_map.csv` (cfpb_company, holding_key, note), `digital_first_companies.csv`
(cfpb_company, digital_segment), `product_groups.csv` (product, sub_product_pattern, product_group),
`state_population.csv` (state, population, source), `state_regions.csv` (state, region, division).

Tests beyond unique/not_null: `accepted_values` on categories; `relationships` complaints→dim_company;
`dbt_utils.accepted_range` on shares (0–1) and z-scores; `expression_is_true` `date_sent_to_company >= received_date` (warn);
no `received_date` in the future; `received_month_id` consistent with `received_date`; singular test
`assert_company_match_rate` warns when matched share of bank-product TTM complaints < 85 %; singular test
`assert_no_duplicate_current_versions`; singular `assert_spike_math` (z within ±50).

Vars: `our_company` (from env `CL_OUR_COMPANY`), `history_start_date: '2011-12-01'`, `timely_window_days: 15`.

## 6. Airflow

Docker Compose, Airflow 2.10, LocalExecutor, Postgres metadata DB, timezone UTC in the scheduler,
DAG `schedule="0 13 * * *"` (13:00 UTC ≈ after the CFPB file refresh around 09:00 UTC and the FRED
daily release). dbt models become individual tasks through **astronomer-cosmos** `DbtTaskGroup`
(one group per target, `render_config.select` by folder, `test_behavior=AFTER_EACH`).

DAGs:
* `conductlens_daily`: `ensure_tables` → [`ingest_cfpb`, `ingest_fdic`, `ingest_fred`] → `dbt_dbx` (TaskGroup: staging, intermediate, marts, snapshot) → `dbt_bq` (TaskGroup) → `parity_check` → `write_evidence`.
  `ingest_*` retries 2× (5 min), `retry_exponential_backoff`. `dbt_bq` is `trigger_rule=all_done` after `dbt_dbx`? No: `all_success`; if Databricks fails, BigQuery still runs because they are independent — the graph is `ingest → {dbt_dbx, dbt_bq}` in parallel, then `parity_check` with `trigger_rule=all_done`.
* `conductlens_weekly_full_diff`: Sundays 14:00 UTC, `cl-ingest run --source cfpb --mode full_diff`.
* `conductlens_backfill`: manual, params `start`, `end`, `sources`.

`parity_check`: for each mart, `count(*)` and a checksum (`sum(complaints)`-style column) on both
warehouses; differences > 0.1 % fail the task and are written to `conductlens_raw._parity_checks`.

## 7. Environment variables (single list)

See `.env.example`. Ingestion reads `CL_*`, `GOOGLE_APPLICATION_CREDENTIALS`, `DATABRICKS_HOST`,
`DATABRICKS_TOKEN` (or `DATABRICKS_CONFIG_PROFILE`), `DATABRICKS_HTTP_PATH`. dbt's `profiles.yml`
reads `CL_BQ_PROJECT`, `CL_BQ_LOCATION`, `GOOGLE_APPLICATION_CREDENTIALS`, `DATABRICKS_HOST`,
`DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`, `CL_DBX_CATALOG`, and the prefixes. No value is ever
logged or printed by any component.

## 8. Dates and time zones

CFPB `Date received` and `Date sent to company` are US-Eastern **calendar dates** with no time
component. They are stored as `DATE` and compared as dates everywhere; nothing converts them.
Weeks start Monday (`received_week`). "This week / this month / YTD" in `mart_volume_periods` are
computed against `current_date` in **US/Eastern** (`cl_today()` macro) so a run at 13:00 UTC and a
run at 23:30 UTC agree on what "today" is. Audit timestamps are UTC.
