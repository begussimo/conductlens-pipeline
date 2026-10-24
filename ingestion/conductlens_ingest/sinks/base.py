"""The sink contract. Every warehouse implements exactly these operations so the runner
does not know which one it is talking to.

Idempotency is the sink's job: `upsert_versions` must write a (key, row_hash) at most once,
no matter how many times the same rows are offered.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol

import pyarrow as pa

from ..schema import TableSpec


@dataclass
class State:
    source: str
    sink: str
    watermark_date: date | None
    source_as_of: date | None
    last_run_id: str | None
    updated_at: datetime | None


@dataclass
class RunLog:
    run_id: str
    source: str
    sink: str
    mode: str
    window_start: date | None
    window_end: date | None
    rows_read: int
    rows_written: int
    status: str
    error: str | None
    started_at: datetime
    finished_at: datetime


class Sink(Protocol):
    name: str

    def ensure_tables(self) -> list[str]:
        """Create what is missing (and on BigQuery rotate what is about to expire). Returns notes."""

    def table_exists(self, table: str) -> bool: ...

    def upsert_versions(self, spec: TableSpec, rows: pa.Table, window: tuple[date, date] | None) -> int:
        """Append rows whose (key, row_hash) is not present. Returns rows actually written."""

    def current_keys(self, spec: TableSpec, window: tuple[date, date] | None) -> pa.Table:
        """Current (latest-version, non-tombstone) key columns + row_hash, optionally within a window."""

    def get_state(self, source: str) -> State | None: ...

    def set_state(self, state: State) -> None: ...

    def log_run(self, run: RunLog) -> None: ...

    def query(self, sql: str) -> pa.Table:
        """Run read-only SQL. `{raw}` and `{marts}` placeholders are expanded to fully-qualified schemas."""

    def append_meta(self, table: str, rows: pa.Table) -> None:
        """Plain append into a metadata table (_parity_checks)."""
