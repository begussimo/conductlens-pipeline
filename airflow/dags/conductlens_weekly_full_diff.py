"""conductlens_weekly_full_diff: Sunday full hash comparison of the CFPB file, then a daily run."""

from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from common import DAILY_DAG_ID, DEFAULT_ARGS, START_DATE, ingest_cmd

DOC = """
## conductlens_weekly_full_diff

Sundays 14:00 UTC (one hour after the daily slot, so it never overlaps the daily
ingest). Runs `cl-ingest run --source cfpb --mode full_diff`: every row of the bulk CSV
is hashed and compared with the raw table, and complaint ids that disappeared from the
file get tombstone rows. This catches edits older than the 60-day incremental window.

It then triggers `conductlens_daily` (without waiting) rather than owning its own dbt
tasks: the daily DAG already knows how to rebuild both warehouses and run the parity
check, and keeping a single dbt graph means one place to fix. With
`max_active_runs_per_dag=1` the triggered run simply queues if a daily run is active.
"""

with DAG(
    dag_id="conductlens_weekly_full_diff",
    description="Weekly CFPB full-diff (tombstones for vanished complaints) then trigger the daily DAG",
    schedule="0 14 * * 0",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "execution_timeout": None},
    doc_md=DOC,
    tags=["conductlens", "weekly"],
) as dag:
    full_diff = BashOperator(
        task_id="ingest_cfpb_full_diff",
        bash_command=ingest_cmd(
            "run", "--source", "cfpb", "--mode", "full_diff", "--run-id", '"{{ run_id }}"'
        ),
        # The full file is 18.7M rows; allow more than the daily 2 h budget.
        execution_timeout=None,
    )

    trigger_daily = TriggerDagRunOperator(
        task_id="trigger_daily",
        trigger_dag_id=DAILY_DAG_ID,
        wait_for_completion=False,
        reset_dag_run=False,
        conf={"triggered_by": "{{ dag.dag_id }}", "upstream_run_id": "{{ run_id }}"},
        retries=0,
    )

    full_diff >> trigger_daily
