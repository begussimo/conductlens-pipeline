"""Write evidence/scheduled_runs.md from _load_runs and _parity_checks on every sink."""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from .sinks import Sink


def write_evidence(sinks: list[Sink], out_dir: Path, limit: int = 40) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    lines = [f"# Scheduled runs (generated {datetime.now(UTC):%Y-%m-%d %H:%M} UTC)", ""]
    for sink in sinks:
        lines += [f"## {sink.name}", ""]
        try:
            runs = sink.query(
                "SELECT run_id, source, mode, status, window_start, window_end, rows_read, rows_written, started_at, "
                f"finished_at, error FROM {{raw}}._load_runs ORDER BY started_at DESC LIMIT {limit}"
            ).to_pylist()
            lines += ["| started (UTC) | run_id | source | mode | status | window | read | written | seconds | error |",
                      "|---|---|---|---|---|---|---:|---:|---:|---|"]
            for r in runs:
                secs = (r["finished_at"] - r["started_at"]).total_seconds() if r["finished_at"] and r["started_at"] else 0
                window = f"{r['window_start']}..{r['window_end']}" if r["window_start"] else "full"
                lines.append(f"| {r['started_at']:%Y-%m-%d %H:%M} | `{r['run_id']}` | {r['source']} | {r['mode']} | {r['status']} | "
                             f"{window} | {r['rows_read']} | {r['rows_written']} | {secs:.0f} | {(r['error'] or '')[:60]} |")
            lines.append("")
            parity = sink.query(
                "SELECT checked_at, table_name, metric, bigquery_value, databricks_value, rel_diff, status "
                f"FROM {{raw}}._parity_checks ORDER BY checked_at DESC LIMIT {limit}"
            ).to_pylist()
            if parity:
                lines += ["### parity checks", "", "| checked (UTC) | table | metric | bigquery | databricks | rel diff | status |",
                          "|---|---|---|---:|---:|---:|---|"]
                for r in parity:
                    lines.append(f"| {r['checked_at']:%Y-%m-%d %H:%M} | {r['table_name']} | {r['metric']} | {r['bigquery_value']:g} | "
                                 f"{r['databricks_value']:g} | {r['rel_diff']:.4%} | {r['status']} |")
                lines.append("")
        except Exception as exc:  # noqa: BLE001
            lines += [f"_unavailable: {exc}_", ""]
    path = out_dir / "scheduled_runs.md"
    path.write_text("\n".join(lines))
    return path
