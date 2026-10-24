# Bootstrap BigQuery (sandbox, no billing)

A checklist rather than a script: the BigQuery sandbox is set up in the console and
there is no billing account to attach a CLI to. Takes about ten minutes.

## 1. Project

1. Open <https://console.cloud.google.com/projectcreate> and create a project
   (e.g. `conductlens-<yourname>`). Note the **project id** (not the display name).
2. Open <https://console.cloud.google.com/bigquery?project=YOUR_PROJECT_ID>. The first
   visit enables the BigQuery API and puts the project in **sandbox** mode
   (banner "Sandbox" top-left). Do not add a billing account; nothing here needs it.

## 2. Service account and key

1. IAM & Admin -> Service Accounts -> **Create service account**, name `conductlens-ingest`.
2. Grant exactly two roles on the project:
   * `BigQuery Data Editor` (create datasets/tables, load jobs)
   * `BigQuery Job User` (run queries and load jobs)
3. Keys tab -> **Add key** -> JSON. Save the downloaded file as
   `secrets/bq-service-account.json` at the repo root:

   ```bash
   mkdir -p secrets && mv ~/Downloads/<project>-*.json secrets/bq-service-account.json
   chmod 600 secrets/bq-service-account.json
   ```

   `secrets/` must stay out of git. The root `.gitignore` ignores `service-account*.json`,
   which does **not** match `bq-service-account.json`; add `secrets/` to `.gitignore` if it
   is not there yet and check with `git status --ignored secrets/` before any commit.

   Alternative for a laptop without a key file: `gcloud auth application-default login`
   and leave `GOOGLE_APPLICATION_CREDENTIALS` empty. Airflow in Docker needs the key file.

## 3. Environment variables

Root `.env` (laptop runs) and `airflow/.env` (container runs):

```
CL_BQ_PROJECT=<project id>
CL_BQ_LOCATION=US
CL_BQ_DATASET_PREFIX=conductlens
GOOGLE_APPLICATION_CREDENTIALS=./secrets/bq-service-account.json     # root .env
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/secrets/bq-service-account.json   # airflow/.env
```

Datasets are **not** created here: `make ensure-tables` (`cl-ingest ensure-tables --sinks bigquery`)
creates `conductlens_raw`, `conductlens_staging`, `conductlens_marts` and the raw tables, and
re-creates expiring ones.

## 4. Verify

```bash
make ensure-tables
.venv/bin/cl-ingest status
```

## Sandbox limits to keep in mind

| limit | consequence for this project |
|---|---|
| No DML (`UPDATE`/`DELETE`/`MERGE`) | raw tables are append-only version logs; `_load_state` is rewritten with a `WRITE_TRUNCATE` load job |
| Tables and partitions expire after **60 days** | `ensure-tables` detects tables expiring in <5 days and `rehydrate` reloads from the latest parquet; Hex is bound to Databricks, not BigQuery |
| **10 GiB** lifetime active storage | intermediate models are views on BigQuery; marts are small aggregates (~10-20 MB/day) |
| **1 TiB/month** query processing | staging views over the partitioned raw table are cheap; `parity_check` runs `count`/`sum` only |
| No streaming inserts, no scheduled queries | everything is load jobs driven from Airflow |
