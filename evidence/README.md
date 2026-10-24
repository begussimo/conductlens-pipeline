# Evidence

Artefacts that show the pipeline actually ran, as opposed to code that says it would.
Everything here is generated; regenerate rather than edit.

| file | what it shows | produced by |
|---|---|---|
| `airflow_runs_<YYYY-MM-DD>.md` | `airflow dags list-runs` for the three DAGs, task states of the latest daily run, `cl-ingest status` (watermarks, last 10 `_load_runs` per sink) | `make evidence` (= `scripts/make_evidence.sh`), stack must be up |
| `scheduled_runs.md` | dump of `conductlens_raw._load_runs` from **both** warehouses: run id, source, sink, mode, window, rows read/written, status, timings | the `write_evidence` task at the end of every `conductlens_daily` run (`cl-ingest evidence`); also `make evidence` |
| `parity_<YYYY-MM-DD>.md` (optional) | `_parity_checks` rows for the day: per-mart counts and checksums on both sides, drift % | `make parity` then copy the printed table |
| `hex/*.png` | screenshots of the Hex app bound to Databricks marts (one per business question) | manual: Hex -> app -> screenshot, name `hex/q<N>_<mart>.png` |
| `airflow_graph.png`, `airflow_grid.png` (optional) | Graph view of `conductlens_daily` with both dbt groups expanded; Grid view with a green run | manual screenshot of <http://localhost:8080> |

## Commands

```bash
make airflow-up                      # start Airflow (first run builds the image)
# wait for a scheduled run or trigger one:
docker compose -f airflow/docker-compose.yml --env-file airflow/.env \
  exec airflow-scheduler airflow dags trigger conductlens_daily
make evidence                        # -> evidence/airflow_runs_<date>.md + evidence/scheduled_runs.md
make parity                          # prints the cross-warehouse comparison, exits non-zero on >0.1% drift
```

`scheduled_runs.md` is written by the container into `/opt/airflow/evidence`, which is this
directory bind-mounted, so it appears here after each daily run without any copy step.
