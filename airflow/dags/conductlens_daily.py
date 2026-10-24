"""conductlens_daily: ingest the three sources, run dbt on both warehouses, compare them."""

from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from cosmos import DbtTaskGroup

from common import DAILY_DAG_ID, DEFAULT_ARGS, START_DATE, dbt_group_kwargs, ingest_cmd

DOC = """
## conductlens_daily

`ensure_tables` -> `ingest.{cfpb,fdic,fred}` -> `dbt_dbx` || `dbt_bq` -> `parity_check` -> `write_evidence`

**Why 13:00 UTC.** The CFPB bulk CSV is refreshed around 09:00 UTC; FRED publishes the
previous business day's observations by 16:00 US/Eastern (20:00/21:00 UTC), so at 13:00
UTC both yesterday's complaints and yesterday's rates are already available, and the
run finishes well before the US business day. FDIC is a full pull and has no timing
constraint.

**Graph rules.** The whole `ingest` group must succeed before either dbt group starts
(dbt reads all three raw tables). `dbt_dbx` and `dbt_bq` are independent: a BigQuery
sandbox expiry or quota error never blocks Databricks and vice versa. `parity_check`
and `write_evidence` run with `trigger_rule=all_done` so the comparison and the evidence
row are written even when one side failed (the parity row then records the gap).

**Recovery in 5 lines.**
1. A red task means one unit of work failed; read its log (task -> Logs tab).
2. Ingest tasks are idempotent (append-only version log keyed by `(id,row_hash)`):
   clearing and re-running never duplicates rows, so *Clear* is always safe.
3. dbt tasks are one model each; clear just that model (and downstream) after a fix.
4. If BigQuery expired (60-day sandbox), `ensure_tables` recreates + rehydrates on the next run.
5. Missed days need no backfill: the incremental window always re-pulls the trailing
   `CL_CFPB_OVERLAP_DAYS` / `CL_FRED_OVERLAP_DAYS`; use `conductlens_backfill` only for older ranges.
"""

with DAG(
    dag_id=DAILY_DAG_ID,
    description="Daily CFPB/FDIC/FRED ingest -> dbt on Databricks and BigQuery -> parity check",
    schedule="0 13 * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    doc_md=DOC,
    tags=["conductlens", "daily"],
) as dag:
    ensure_tables = BashOperator(
        task_id="ensure_tables",
        bash_command=ingest_cmd("ensure-tables"),
        doc_md="Creates datasets/schemas/volume/tables if missing; rotates/rehydrates expiring BQ tables.",
    )

    with TaskGroup(group_id="ingest", tooltip="cl-ingest run --mode incremental per source") as ingest:
        for source in ("cfpb", "fdic", "fred"):
            BashOperator(
                task_id=f"ingest_{source}",
                bash_command=ingest_cmd(
                    "run", "--source", source, "--mode", "incremental", "--run-id", '"{{ run_id }}"'
                ),
            )

    dbt_dbx = DbtTaskGroup(group_id="dbt_dbx", **dbt_group_kwargs("dbx"))
    dbt_bq = DbtTaskGroup(group_id="dbt_bq", **dbt_group_kwargs("bq"))

    parity_check = BashOperator(
        task_id="parity_check",
        bash_command=ingest_cmd("parity-check"),
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
        doc_md=(
            "Compares every mart's count/checksum across warehouses; "
            "writes `_parity_checks`; fails on >0.1% drift."
        ),
    )

    write_evidence = BashOperator(
        task_id="write_evidence",
        bash_command=ingest_cmd("evidence"),
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
        doc_md="Dumps `_load_runs` from both sinks into evidence/scheduled_runs.md (bind mount).",
    )

    ensure_tables >> ingest
    ingest >> dbt_dbx
    ingest >> dbt_bq
    [dbt_dbx, dbt_bq] >> parity_check >> write_evidence
