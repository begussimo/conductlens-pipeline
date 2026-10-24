"""Compare the dbt marts across the two warehouses: row counts and sums of numeric columns.
Writes one row per (table, metric) into _parity_checks on both sinks; returns the failures."""
from __future__ import annotations

import logging
from datetime import UTC, datetime

import pyarrow as pa

from .config import Settings
from .schema import PARITY_SCHEMA
from .sinks import Sink

log = logging.getLogger(__name__)
NUMERIC = {"INT64", "FLOAT64", "NUMERIC", "BIGNUMERIC", "BIGINT", "INT", "DOUBLE", "FLOAT", "DECIMAL", "SMALLINT", "TINYINT"}


def _columns(sink: Sink, settings: Settings) -> dict[str, list[str]]:
    if sink.name == "bigquery":
        sql = "SELECT table_name, column_name, data_type FROM {marts}.INFORMATION_SCHEMA.COLUMNS"
    elif sink.name == "databricks":
        sql = (f"SELECT table_name, column_name, data_type FROM `{settings.dbx_catalog}`.information_schema.columns "
               f"WHERE table_schema = '{settings.dbx_prefix}_marts'")
    else:
        sql = "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'marts'"
    out: dict[str, list[str]] = {}
    for r in sink.query(sql).to_pylist():
        t = r["table_name"]
        out.setdefault(t, [])
        if str(r["data_type"]).upper().split("(")[0] in NUMERIC:
            out[t].append(r["column_name"])
    return out


def _metrics(sink: Sink, table: str, numeric_cols: list[str]) -> dict[str, float]:
    sums = ", ".join(f"SUM(CAST({c} AS DOUBLE)) AS s_{c}" for c in numeric_cols) if numeric_cols else ""
    sql = "SELECT COUNT(*) AS n" + (", " + sums if sums else "") + " FROM {marts}." + table
    if sink.name == "bigquery":
        sql = sql.replace("AS DOUBLE", "AS FLOAT64")
    row = sink.query(sql).to_pylist()[0]
    return {"count": float(row["n"]), **{f"sum_{c}": float(row[f"s_{c}"] or 0.0) for c in numeric_cols}}


def parity_check(settings: Settings, sinks: list[Sink], run_id: str, tolerance: float = 0.001) -> list[str]:
    by_name = {s.name: s for s in sinks}
    if not {"bigquery", "databricks"} <= set(by_name):
        raise RuntimeError("parity check needs both the bigquery and databricks sinks")
    bq, dbx = by_name["bigquery"], by_name["databricks"]
    cols_bq, cols_dbx = _columns(bq, settings), _columns(dbx, settings)
    common = sorted(set(cols_bq) & set(cols_dbx))
    missing = sorted(set(cols_bq) ^ set(cols_dbx))
    rows, failures = [], []
    now = datetime.now(UTC)
    for t in missing:
        failures.append(f"{t}: present on only one warehouse")
        rows.append({"run_id": run_id, "table_name": t, "metric": "exists", "bigquery_value": float(t in cols_bq),
                     "databricks_value": float(t in cols_dbx), "rel_diff": 1.0, "status": "failed", "checked_at": now})
    for t in common:
        numeric = sorted(set(cols_bq[t]) & set(cols_dbx[t]))
        m_bq, m_dbx = _metrics(bq, t, numeric), _metrics(dbx, t, numeric)
        for metric in m_bq:
            a, b = m_bq[metric], m_dbx.get(metric, float("nan"))
            denom = max(abs(a), abs(b), 1e-9)
            rel = abs(a - b) / denom
            status = "ok" if rel <= tolerance else "failed"
            if status == "failed":
                failures.append(f"{t}.{metric}: bigquery={a:g} databricks={b:g} rel_diff={rel:.4%}")
            rows.append({"run_id": run_id, "table_name": t, "metric": metric, "bigquery_value": a, "databricks_value": b,
                         "rel_diff": rel, "status": status, "checked_at": now})
    table = pa.Table.from_pylist(rows, schema=PARITY_SCHEMA)
    for s in (bq, dbx):
        try:
            s.append_meta("_parity_checks", table)
        except Exception as exc:  # noqa: BLE001
            log.error("%s: could not write _parity_checks: %s", s.name, exc)
    log.info("parity: %d tables compared, %d failures", len(common), len(failures))
    return failures
