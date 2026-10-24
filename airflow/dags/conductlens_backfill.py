"""conductlens_backfill: manual, parameterised historical load, then a daily run to rebuild dbt."""

from __future__ import annotations

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from common import DAILY_DAG_ID, DEFAULT_ARGS, START_DATE, ingest_cmd

DOC = """
## conductlens_backfill (manual)

Trigger with config, e.g. `{"start": "2024-01-01", "end": "2024-06-30", "sources": "cfpb,fred"}`.

One task per source runs `cl-ingest run --source <s> --mode backfill --start .. --end ..
--sinks <sinks>`. A source not listed in `sources` exits 99, which Airflow records as
*skipped* (not failed), so the trigger step still runs (`trigger_rule=none_failed`).
`fdic` is accepted for completeness: it is always a full pull, the window is ignored.

Backfill raises the watermark only if `end` is newer than it, so backfilling an old
range never makes the next incremental run skip anything. Re-running the same window
writes nothing (hashes already present): safe to repeat. The final step triggers
`conductlens_daily` so dbt rebuilds both warehouses on top of the new history.
"""

SOURCES = ("cfpb", "fdic", "fred")

with DAG(
    dag_id="conductlens_backfill",
    description="Manual historical load for a date window, then trigger the daily DAG",
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "execution_timeout": None},
    params={
        "start": Param(
            "2024-01-01", type="string", format="date", description="Window start (inclusive, YYYY-MM-DD)"
        ),
        "end": Param(
            "2024-12-31", type="string", format="date", description="Window end (inclusive, YYYY-MM-DD)"
        ),
        "sources": Param("cfpb,fred", type="string", description="Comma list from cfpb,fdic,fred"),
        "sinks": Param(
            "bigquery,databricks", type="string", description="Comma list from bigquery,databricks"
        ),
    },
    doc_md=DOC,
    tags=["conductlens", "manual"],
) as dag:
    with TaskGroup(group_id="backfill") as backfill:
        for source in SOURCES:
            BashOperator(
                task_id=f"backfill_{source}",
                # Exit 99 -> AirflowSkipException (BashOperator default skip_on_exit_code).
                bash_command=(
                    f'case ",{{{{ params.sources }}}}," in *",{source},"*) ;; *) '
                    f'echo "{source} not in params.sources; skipping"; exit 99 ;; esac\n'
                    + ingest_cmd(
                        "run",
                        "--source",
                        source,
                        "--mode",
                        "backfill",
                        "--start",
                        "{{ params.start }}",
                        "--end",
                        "{{ params.end }}",
                        "--sinks",
                        "{{ params.sinks }}",
                        "--run-id",
                        '"{{ run_id }}"',
                    )
                ),
            )

    trigger_daily = TriggerDagRunOperator(
        task_id="trigger_daily",
        trigger_dag_id=DAILY_DAG_ID,
        wait_for_completion=False,
        reset_dag_run=False,
        conf={"triggered_by": "{{ dag.dag_id }}", "upstream_run_id": "{{ run_id }}"},
        trigger_rule=TriggerRule.NONE_FAILED,
        retries=0,
    )

    backfill >> trigger_daily
