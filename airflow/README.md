# Airflow for ConductLens

Apache Airflow 2.10.5, LocalExecutor, Postgres 16, in Docker Compose. dbt models are
rendered as individual tasks with astronomer-cosmos. Contract: `docs/ARCHITECTURE.md` §6.

## Layout

```
airflow/
  docker-compose.yml     postgres, airflow-init, airflow-webserver (:8080), airflow-scheduler
  Dockerfile             apache/airflow:2.10.5-python3.12 + Cosmos; dbt + cl-ingest in /opt/airflow/dbt_venv
  requirements.txt       Airflow env: astronomer-cosmos only
  requirements-dbt.txt   dbt venv: dbt-core, dbt-bigquery, dbt-databricks
  .env.example           copy to .env (gitignored); container paths
  dags/common.py         default_args, failure callback, Cosmos configs, env helpers
  dags/conductlens_daily.py
  dags/conductlens_weekly_full_diff.py
  dags/conductlens_backfill.py
  logs/                  task logs (gitignored, bind-mounted)
```

Two Python environments in the image, on purpose:

| env | contents | why |
|---|---|---|
| Airflow (`/home/airflow/.local`) | airflow 2.10.5, astronomer-cosmos | DAG parsing + operators |
| `/opt/airflow/dbt_venv` | dbt-core/bigquery/databricks, `conductlens_ingest` (`cl-ingest`) | dbt and the ingestion package share heavy deps (dbt is a dependency in `pyproject.toml`); keeping them out of the Airflow env avoids version clashes. Cosmos only needs the dbt *path*; DAGs call `cl-ingest` through `BashOperator`, never import it. |

The build context is the **repo root** because `pyproject.toml` lives there; the Dockerfile
copies `pyproject.toml` + `ingestion/` to `/opt/airflow/repo` and does `pip install -e`.
At runtime `../ingestion` is bind-mounted over `/opt/airflow/repo/ingestion`, so code edits
are live without a rebuild (rebuild only when dependencies change).

Bind mounts: `../dbt -> /opt/airflow/dbt`, `../data -> /opt/airflow/data` (landing zone,
`CL_DATA_DIR`), `../evidence -> /opt/airflow/evidence`, `../secrets -> /opt/airflow/secrets`
(read-only; holds `bq-service-account.json`), `./dags`, `./logs`.

## Start

```bash
cp airflow/.env.example airflow/.env      # fill CL_BQ_PROJECT, DATABRICKS_*, _AIRFLOW_WWW_USER_PASSWORD
make airflow-up                           # builds the image (~5 min first time), runs db migrate, creates the admin user
open http://localhost:8080                # user from _AIRFLOW_WWW_USER_USERNAME / _PASSWORD
make airflow-logs                         # scheduler + webserver logs
make airflow-dag-test                     # import errors + dag list inside the container
make airflow-down                         # stop; `docker compose ... down -v` wipes the metadata DB
```

DAGs start **paused**. Unpause `conductlens_daily` and `conductlens_weekly_full_diff` in the UI
once `make ensure-tables` has succeeded at least once from the laptop or via a manual run.

All `CL_*`, `DATABRICKS_*`, `DBT_*` and `AIRFLOW__*` variables come from `airflow/.env`
(`env_file`), with container paths pinned in `docker-compose.yml`. No secret is baked into
the image or written to a DAG.

## DAGs

| DAG | schedule (UTC) | what |
|---|---|---|
| `conductlens_daily` | `0 13 * * *` | `ensure_tables` -> `ingest.{cfpb,fdic,fred}` -> `dbt_dbx` and `dbt_bq` in parallel -> `parity_check` -> `write_evidence` |
| `conductlens_weekly_full_diff` | `0 14 * * 0` | CFPB full-file hash diff with tombstones, then triggers `conductlens_daily` |
| `conductlens_backfill` | manual | `params`: `start`, `end`, `sources` (default `cfpb,fred`), `sinks`; one task per source, then triggers `conductlens_daily` |

Why 13:00 UTC, the independence of the two dbt groups and the recovery story are in each
DAG's `doc_md` (UI -> DAG -> Docs). Retries: 2, 5 min, exponential backoff capped at 30 min;
`execution_timeout` 2 h for daily tasks (none for full-diff/backfill, which are long by nature).

### Cosmos

Both dbt groups use `ProfileConfig(profiles_yml_filepath=/opt/airflow/dbt/profiles.yml,
profile_name="conductlens", target_name="dbx"|"bq")`, `ExecutionConfig(dbt_executable_path=
/opt/airflow/dbt_venv/bin/dbt, invocation_mode=SUBPROCESS)` and
`RenderConfig(select=[path:models/staging, path:models/intermediate, path:models/marts, (dbx only) path:snapshots],
test_behavior=AFTER_EACH)`. Each model is `<model>_run` followed by `<model>_test`.

`COSMOS_LOAD_METHOD` picks how the graph is discovered at parse time:

* `dbt_ls` (default): `dbt ls` with the real binary; the scheduler needs the warehouse env vars.
* `dbt_manifest`: read `dbt/target/manifest.json`; run `dbt parse` first. Fastest, no warehouse needed at parse time.
* `custom`: Cosmos' own SQL parser; no dbt at all. Used by the import test (see below).

### Failure callback

`common.on_failure_callback` logs one JSON line `CONDUCTLENS_ALERT {...}` (dag, task, run_id,
try number, log URL, exception) to the task log and scheduler log. No SMTP dependency; the
`AIRFLOW__SMTP__*` placeholders in the compose file are commented out and can be enabled later.

## Where the logs are

* Task logs: UI -> task square -> *Logs*, or on disk in `airflow/logs/dag_id=.../run_id=.../task_id=.../attempt=N.log`.
* Scheduler/webserver: `make airflow-logs`.
* dbt output is in the task log of each `<model>_run` / `<model>_test` task.
* Ingestion audit trail: `conductlens_raw._load_runs` on both warehouses (`cl-ingest status`, or `evidence/scheduled_runs.md`).

## What a failure looks like

1. **Red square** in the Grid view, e.g. `dbt_bq.mart_company_benchmark_run`. The daily run
   continues: `dbt_dbx` is independent, `parity_check` and `write_evidence` have
   `trigger_rule=all_done`, and `CONDUCTLENS_ALERT` is in the scheduler log.
2. **Read the log**: click the square -> *Logs*. The last `attempt=N.log` holds the dbt or
   `cl-ingest` stderr. Typical causes: BigQuery sandbox table expired, Databricks warehouse
   auto-stopped and timed out, FRED/CFPB endpoint 5xx, a dbt test failing on new data.
3. **Fix** the cause (edit the model / seed, extend a quota, restart the warehouse). Edits
   to `dbt/` and `ingestion/` are bind-mounted, so no rebuild.
4. **Clear the task** (square -> *Clear* -> *Downstream* to also re-run dependants).
   Clearing is always safe: ingestion appends only `(id,row_hash)` pairs that are not there
   yet, so a re-run writes nothing twice; dbt tables are rebuilt whole; `_load_runs` gets a
   new row with the same `run_id`.
5. A whole missed day needs nothing: the next incremental run re-pulls the trailing
   overlap window (`CL_CFPB_OVERLAP_DAYS`, `CL_FRED_OVERLAP_DAYS`).

## How a backfill works

UI -> `conductlens_backfill` -> *Trigger DAG w/ config*:

```json
{"start": "2023-01-01", "end": "2023-12-31", "sources": "cfpb,fred", "sinks": "bigquery,databricks"}
```

Each listed source runs `cl-ingest run --source <s> --mode backfill --start .. --end .. --sinks ..`;
unlisted sources are *skipped* (exit 99). Backfill raises the watermark only if `end` is newer,
so backfilling history never makes incremental runs skip recent data. When done it triggers
`conductlens_daily` so both warehouses are rebuilt by dbt and compared. From the laptop:
`make backfill START=2023-01-01 END=2023-12-31`.

## Validating DAG files without Docker

```bash
uv venv /tmp/afvenv --python 3.12
uv pip install --python /tmp/afvenv "apache-airflow==2.10.5" "astronomer-cosmos>=1.9,<2" \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt
export AIRFLOW_HOME=/tmp/afhome AIRFLOW__CORE__LOAD_EXAMPLES=False
export CL_DBT_PROJECT_DIR=$PWD/dbt CL_DBT_EXECUTABLE=dbt CL_INGEST_BIN=cl-ingest COSMOS_LOAD_METHOD=custom
/tmp/afvenv/bin/airflow db migrate -q
AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow/dags /tmp/afvenv/bin/airflow dags list-import-errors
```

`COSMOS_LOAD_METHOD=custom` lets Cosmos build the graph from the SQL files alone, so the
import check needs neither dbt nor warehouse credentials.
