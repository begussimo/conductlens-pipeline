# ConductLens dbt project

One dbt project (`conductlens`, profile `conductlens`) that runs unchanged on two adapters:

| target | adapter | role |
|---|---|---|
| `dbx` (default) | dbt-databricks | Databricks Free Edition, the durable replica: everything runs, including the FDIC snapshot |
| `bq` | dbt-bigquery | BigQuery sandbox: no DML (no incremental, no snapshot), 60-day table expiry, lifetime 10 GiB write quota |

## Run

```bash
cp ../.env.example ../.env   # fill in; profiles.yml reads only env vars, nothing is written here
export $(grep -v '^#' ../.env | xargs)   # or use direnv / dotenv
cd dbt
uv run dbt deps
uv run dbt build --target dbx                       # seeds, models, snapshot, tests
uv run dbt build --target bq --exclude resource_type:snapshot   # snapshot is disabled on bq anyway; --exclude keeps logs clean
uv run dbt source freshness --target dbx
uv run dbt docs generate && uv run dbt docs serve
```

`DBT_TARGET=bq|dbx` selects the default target; `DBT_PROFILES_DIR=./dbt` (from the repo root) points at
`profiles.yml`. Offline checks that need no warehouse: `dbt parse --target dbx`, `dbt parse --target bq`,
`dbt ls --target bq --resource-type model`.

## What differs per target

| folder | Databricks | BigQuery (sandbox) |
|---|---|---|
| staging | view | view |
| intermediate | table (Delta; `int_complaints_daily` liquid-clustered by `received_date, company`) | **view** (no DML and every table write counts against the lifetime quota) |
| marts | table | table (small aggregates: TTM or trailing-36-month windows, companies above a volume floor; ~3 MB/day in total) |
| snapshot `snap_fdic_institutions` | runs (`check` strategy) | `enabled: false` (needs MERGE) |

Implemented by one macro, `cl_materialized(default)`, called from every model's `config()`.

**Raw CFPB grain differs** (ARCHITECTURE §3.1 / §3.1b). Databricks lands `cfpb_complaints` at complaint
grain; BigQuery lands `cfpb_complaints_daily` (daily cells with integer codes, decoded through `cfpb_codes`)
because 18.7M complaint rows would cost 7.7 GB of the 10 GiB lifetime write budget per 60-day rotation.
So:

* `stg_cfpb_complaints` exists only on `dbx`; `stg_cfpb_complaints_daily` exists only on `bq`. The switch is
  the macro `cl_cfpb_grain()`: BigQuery reads `CL_BQ_CFPB_GRAIN` (default `daily_cells`, `complaints` to land the
  full log instead), every other target uses `complaints` (dbt var `cl_cfpb_grain: daily_cells` forces the cell
  path for smoke tests).
* `int_complaints_daily` has the same grain and columns on both targets (`received_date, company, product,
  sub_product, issue, state, company_response, timely_response, consumer_disputed` + `n_complaints`):
  a `group by` over complaints on Databricks, a pass-through of the daily cells on BigQuery.
* **Every mart reads only `int_complaints_daily`** and sums `n_complaints`; mart SQL is byte-identical on both.
* BigQuery loses `zip_code`, `tags`, `submitted_via`, `sub_issue`, `date_sent_to_company`,
  `company_public_response`; none is needed by the ten questions.

Schemas: `generate_schema_name` returns `<prefix>_<folder schema>` verbatim (`conductlens_staging`,
`conductlens_marts`, `conductlens_snapshots`) where the prefix is `CL_BQ_DATASET_PREFIX` /
`CL_DBX_SCHEMA_PREFIX`. Sources live in `<prefix>_raw`.

## Portability rules

* Dialect differences go through dbt cross-database macros (`dbt.date_trunc`, `dbt.dateadd`, `dbt.datediff`,
  `dbt.type_string`, …) or the custom dispatch macros in `macros/`: `safe_divide`, `cl_today` (today in
  US/Eastern), `cl_week_start` (Monday weeks; BigQuery's `DATE_TRUNC(…, WEEK)` is Sunday-based by default),
  `cl_month_start`, `cl_date_shift`, `cl_numbers` (integer series), `normalize_company_name`, `cl_size_bucket`.
* No `SAFE.`, `STRUCT`, `ARRAY_AGG`, `PIVOT`, `DATE_DIFF`, backticks, `||`, `ILIKE`.
* Databricks-only extras are guarded by `target.type == 'databricks'` (liquid clustering, snapshot).

## Layout

```
dbt_project.yml  profiles.yml  packages.yml (dbt_utils >= 1.3)
macros/          cross-database helpers (see above) + generate_schema_name
models/sources.yml           conductlens_raw sources with freshness (warn 36 h, error 72 h)
models/staging/              stg_cfpb_complaints (dbx) | stg_cfpb_complaints_daily (bq), stg_fdic_institutions, stg_fred_observations
models/intermediate/         int_product_groups, int_company_mapping, int_complaints_daily, int_fdic_deposits_by_holding, int_rates_daily, int_complaints_weekly
models/marts/                dim_company + 13 question marts (see ARCHITECTURE §5)
models/exposures.yml         hex_conductlens_dashboard
seeds/                       product_groups, company_fdic_map, digital_first_companies, state_population, state_regions
snapshots/                   snap_fdic_institutions (Databricks only)
tests/                       assert_company_match_rate (warn < 85 %), assert_no_duplicate_current_versions, assert_spike_math, assert_product_groups_matched (warn)
```

## Vars

| var | default | used by |
|---|---|---|
| `our_company` | `env CL_OUR_COMPANY` (`ALLY FINANCIAL INC.`) | `dim_company.is_our_company`, benchmark highlight |
| `history_start_date` | `2011-12-01` | `int_rates_daily` spine |
| `timely_window_days` | 15 | documented basis of `is_timely` |
| `company_monthly_history_months` | 36 | window of the month × company / issue marts |
| `company_min_complaints_ttm` | 100 | company floor for timeliness / resolution marts |
| `spike_weeks_kept`, `spike_min_window_volume` | 60, 40 | `mart_weekly_spikes` |
| `max_lag_weeks`, `rates_corr_start_date` | 26, `2012-01-01` | `mart_rates_lag_correlation` |
