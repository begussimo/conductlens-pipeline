"""Shared pieces for the ConductLens DAGs: default_args, env helpers, failure callback,
Cosmos configs. Imported by every DAG file; listed in .airflowignore so the scheduler
does not try to parse it as a DAG.

Nothing in here imports `conductlens_ingest`: the ingestion package lives in the dbt
venv and is only ever called through BashOperator (`cl-ingest ...`).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from cosmos import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode, TestBehavior

log = logging.getLogger("conductlens.airflow")

# --------------------------------------------------------------------------- paths / env
# Every path is env-overridable so the same DAG files import outside Docker
# (tests, `airflow dags list` in a laptop venv) and inside the container.
DBT_PROJECT_DIR = Path(os.environ.get("CL_DBT_PROJECT_DIR", "/opt/airflow/dbt"))
DBT_PROFILES_PATH = Path(os.environ.get("CL_DBT_PROFILES_PATH", str(DBT_PROJECT_DIR / "profiles.yml")))
DBT_EXECUTABLE = os.environ.get("CL_DBT_EXECUTABLE", "/opt/airflow/dbt_venv/bin/dbt")
INGEST_BIN = os.environ.get("CL_INGEST_BIN", "/opt/airflow/dbt_venv/bin/cl-ingest")
DBT_PROFILE_NAME = "conductlens"
DAILY_DAG_ID = "conductlens_daily"
START_DATE = pendulum.datetime(2026, 10, 20, tz="UTC")


def ingest_cmd(*args: str) -> str:
    """Build a `cl-ingest ...` shell line. Jinja in args is left for Airflow to render."""
    return " ".join([INGEST_BIN, *args])


# --------------------------------------------------------------------------- failure callback
def on_failure_callback(context: dict) -> None:
    """Log one JSON line per failed task instance. Grep-able from the scheduler logs and
    cheap to forward to any alerting sink later; no SMTP or Slack dependency."""
    ti = context.get("task_instance")
    exc = context.get("exception")
    payload = {
        "event": "task_failed",
        "dag_id": getattr(ti, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": context.get("run_id"),
        "try_number": getattr(ti, "try_number", None),
        "max_tries": getattr(ti, "max_tries", None),
        "logical_date": str(context.get("logical_date") or context.get("execution_date")),
        "log_url": getattr(ti, "log_url", None),
        "exception": (str(exc)[:500] if exc else None),
    }
    log.error("CONDUCTLENS_ALERT %s", json.dumps(payload, default=str))


DEFAULT_ARGS = {
    "owner": "conductlens",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": on_failure_callback,
}

# --------------------------------------------------------------------------- Cosmos
# How Cosmos discovers the dbt graph at DAG-parse time (COSMOS_LOAD_METHOD):
#   dbt_ls       (default, production) runs `dbt ls` with the real dbt binary; needs the
#                dbt venv and the warehouse env vars present in the scheduler container.
#   dbt_manifest reads dbt/target/manifest.json (run `dbt parse` first); no dbt call.
#   custom       Cosmos' own SQL parser; no dbt binary, no manifest. Used by the import
#                test so the DAG files can be validated in a plain Airflow venv.
_LOAD_METHODS = {
    "dbt_ls": LoadMode.DBT_LS,
    "dbt_manifest": LoadMode.DBT_MANIFEST,
    "custom": LoadMode.CUSTOM,
}
LOAD_METHOD = _LOAD_METHODS[os.environ.get("COSMOS_LOAD_METHOD", "dbt_ls").lower()]

PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=DBT_PROJECT_DIR,
    manifest_path=(DBT_PROJECT_DIR / "target" / "manifest.json")
    if LOAD_METHOD == LoadMode.DBT_MANIFEST
    else None,
    project_name=DBT_PROFILE_NAME,
)

EXECUTION_CONFIG = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    # SUBPROCESS: dbt is in its own venv, so it must not be imported into the Airflow process.
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path=DBT_EXECUTABLE,
)

# One operator_args dict for both groups. Cosmos defaults append_env=True, so the dbt
# subprocess inherits the container env (CL_*, DATABRICKS_*, GOOGLE_APPLICATION_CREDENTIALS).
DBT_OPERATOR_ARGS = {
    "install_deps": True,
    "append_env": True,
}

# Selected by folder so every model / snapshot is its own Airflow task. Snapshots are
# disabled in-project on BigQuery, so `path:snapshots` is only listed for dbx.
DBT_SELECT = {
    "dbx": ["path:models/staging", "path:models/intermediate", "path:models/marts", "path:snapshots"],
    "bq": ["path:models/staging", "path:models/intermediate", "path:models/marts"],
}


def profile_config(target: str) -> ProfileConfig:
    return ProfileConfig(
        profile_name=DBT_PROFILE_NAME,
        target_name=target,
        profiles_yml_filepath=DBT_PROFILES_PATH,
    )


def render_config(target: str) -> RenderConfig:
    return RenderConfig(
        select=DBT_SELECT[target],
        test_behavior=TestBehavior.AFTER_EACH,
        load_method=LOAD_METHOD,
        invocation_mode=InvocationMode.SUBPROCESS,
        dbt_executable_path=DBT_EXECUTABLE,
        dbt_deps=True,
        emit_datasets=False,
    )


def dbt_group_kwargs(target: str) -> dict:
    """Everything a `DbtTaskGroup(group_id=f'dbt_{target}', **kwargs)` needs."""
    return {
        "project_config": PROJECT_CONFIG,
        "profile_config": profile_config(target),
        "execution_config": EXECUTION_CONFIG,
        "render_config": render_config(target),
        "operator_args": DBT_OPERATOR_ARGS,
        "default_args": DEFAULT_ARGS,
    }
