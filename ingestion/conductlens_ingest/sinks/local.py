"""DuckDB file sink: the same contract as the warehouses, for tests and laptop dry runs."""
from __future__ import annotations

from datetime import UTC, date, datetime

import duckdb
import pyarrow as pa

from ..config import Settings
from ..schema import META_TABLES, TABLES, TableSpec, duckdb_columns_ddl
from ._common import anti_join_new_versions, current_version_sql, window_predicate
from .base import RunLog, State


class LocalSink:
    name = "local"

    def __init__(self, settings: Settings, path: str | None = None, cfpb_grain: str = "complaints"):
        self.cfpb_grain = cfpb_grain
        p = path or str(settings.local_db_path)
        if p != ":memory:":
            settings.local_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(p)
        self.con.execute("SET TimeZone = 'UTC'")
        self.con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        self.con.execute("CREATE SCHEMA IF NOT EXISTS marts")

    # -------------------------------------------------------------- ddl
    def ensure_tables(self) -> list[str]:
        notes = []
        for spec in TABLES.values():
            self.con.execute(f'CREATE TABLE IF NOT EXISTS raw."{spec.name}" ({duckdb_columns_ddl(spec.schema)})')
            notes.append(f"local: ensured raw.{spec.name}")
        for name, schema in META_TABLES.items():
            self.con.execute(f'CREATE TABLE IF NOT EXISTS raw."{name}" ({duckdb_columns_ddl(schema)})')
        return notes

    def table_exists(self, table: str) -> bool:
        r = self.con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema='raw' AND table_name=?", [table]
        ).fetchone()
        return bool(r and r[0])

    # -------------------------------------------------------------- data
    def current_keys(self, spec: TableSpec, window: tuple[date, date] | None) -> pa.Table:
        sql = current_version_sql(spec, f'raw."{spec.name}"', window_predicate(spec, window))
        return self.con.execute(sql).to_arrow_table()

    def upsert_versions(self, spec: TableSpec, rows: pa.Table, window: tuple[date, date] | None) -> int:
        new = anti_join_new_versions(spec, rows, self.current_keys_all_versions(spec, window))
        if new.num_rows:
            self.con.register("_incoming", new)
            cols = ", ".join(f'"{f.name}"' for f in spec.schema)
            self.con.execute(f'INSERT INTO raw."{spec.name}" ({cols}) SELECT {cols} FROM _incoming')
            self.con.unregister("_incoming")
        return new.num_rows

    def current_keys_all_versions(self, spec: TableSpec, window) -> pa.Table:
        keys = ", ".join(f'"{k}"' for k in spec.key_columns)
        return self.con.execute(
            f'SELECT {keys}, "row_hash" FROM raw."{spec.name}" WHERE {window_predicate(spec, window)}'
        ).to_arrow_table()

    # -------------------------------------------------------------- meta
    def get_state(self, source: str) -> State | None:
        r = self.con.execute(
            'SELECT source, sink, watermark_date, source_as_of, last_run_id, updated_at FROM raw."_load_state" '
            "WHERE source=? AND sink=?", [source, self.name]
        ).fetchone()
        return State(*r) if r else None

    def set_state(self, state: State) -> None:
        self.con.execute('DELETE FROM raw."_load_state" WHERE source=? AND sink=?', [state.source, self.name])
        self.con.execute(
            'INSERT INTO raw."_load_state" VALUES (?, ?, ?, ?, ?, ?)',
            [state.source, self.name, state.watermark_date, state.source_as_of, state.last_run_id,
             datetime.now(UTC)],
        )

    def log_run(self, run: RunLog) -> None:
        self.con.execute(
            'INSERT INTO raw."_load_runs" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [run.run_id, run.source, self.name, run.mode, run.window_start, run.window_end, run.rows_read,
             run.rows_written, run.status, run.error, run.started_at, run.finished_at],
        )

    def append_meta(self, table: str, rows: pa.Table) -> None:
        self.con.register("_meta", rows)
        self.con.execute(f'INSERT INTO raw."{table}" SELECT * FROM _meta')
        self.con.unregister("_meta")

    def query(self, sql: str) -> pa.Table:
        return self.con.execute(sql.format(raw="raw", marts="marts")).to_arrow_table()
