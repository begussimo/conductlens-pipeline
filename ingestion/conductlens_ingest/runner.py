"""Run modes, windows and watermarks. One Runner drives N sinks; a failure in one sink is
logged to that sink's _load_runs (when possible), the other sinks still run, and the
failure is re-raised at the end so the orchestrator sees red.

Every CFPB write goes through `_load_cfpb_window`: for each calendar-quarter chunk of the
window it (1) derives the rows the sink wants — complaint-level versions, or daily cells
when the sink's `cfpb_grain` is `daily_cells` (BigQuery) — (2) upserts them, and (3) writes
tombstones for keys the warehouse currently has in that chunk but the file no longer does.
`full_diff` is simply that procedure over the whole history.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa

from .codes import CodeBook, distinct_values, encode_cells
from .config import Settings
from .hashing import hash_expression
from .landing import Landing
from .schema import TABLES, TableSpec
from .sinks import Sink, make_sink
from .sinks.base import RunLog, State
from .sources import cfpb, fdic, fred

log = logging.getLogger(__name__)
MODES = ("incremental", "backfill", "full_diff")
AUDIT_COLUMNS = {"row_hash", "_source", "_source_as_of", "_loaded_at", "_run_id", "_is_tombstone"}


class SinkFailure(RuntimeError):
    pass


@dataclass
class Audit:
    source: str
    as_of: date | None
    run_id: str
    loaded_at: datetime


def _quarters(start: date, end: date):
    """Calendar-quarter chunks covering [start, end], to bound memory on big windows."""
    cur = start
    while cur <= end:
        q_end_month = ((cur.month - 1) // 3 + 1) * 3
        nxt = date(cur.year + (q_end_month == 12), 1 if q_end_month == 12 else q_end_month + 1, 1)
        yield cur, min(end, nxt - timedelta(days=1))
        cur = nxt


def _audit_sql(a: Audit, schema: pa.Schema) -> str:
    as_of = f"DATE '{a.as_of.isoformat()}'" if a.as_of else "NULL::DATE"
    parts = [
        f"'{a.source}' AS _source", f"{as_of} AS _source_as_of",
        f"TIMESTAMPTZ '{a.loaded_at.astimezone(UTC):%Y-%m-%d %H:%M:%S.%f}+00' AS _loaded_at",
    ]
    if "_run_id" in schema.names:
        parts.append(f"'{a.run_id}' AS _run_id")
    parts.append("false AS _is_tombstone")
    return ", ".join(parts)


def _grain(sink: Sink) -> str:
    return getattr(sink, "cfpb_grain", "complaints")


class Runner:
    def __init__(self, settings: Settings, sink_names: list[str] | None = None, run_id: str | None = None,
                 sinks: list[Sink] | None = None):
        self.settings = settings
        self.landing = Landing(settings)
        self.run_id = run_id or f"local-{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
        self.sinks: list[Sink] = sinks if sinks is not None else [make_sink(n, settings) for n in (sink_names or settings.sinks)]
        self.today = datetime.now(UTC).date()
        self._codebooks: dict[str, CodeBook] = {}

    # ------------------------------------------------------------------ row derivation
    def _versions(self, business: pa.Table | Path, spec: TableSpec, audit: Audit, where: str = "1=1") -> pa.Table:
        """Business rows (table or parquet path) -> one row per key with row_hash + audit columns."""
        con = duckdb.connect()
        con.execute("SET TimeZone = 'UTC'")
        if isinstance(business, Path):
            src = f"read_parquet('{business.as_posix()}')"
        else:
            con.register("business", business)
            src = "business"
        cols = ", ".join(f'"{f.name}"' for f in spec.schema if f.name not in AUDIT_COLUMNS)
        keys = ", ".join(f'"{k}"' for k in spec.key_columns)
        sql = f"""
            SELECT {cols}, {hash_expression(spec.schema, spec.hash_columns, spec.hash_length)} AS row_hash,
                   {_audit_sql(audit, spec.schema)}
            FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY 1) AS _rn FROM {src} WHERE {where}) d
            WHERE _rn = 1
        """
        out = con.execute(sql).to_arrow_table()
        con.close()
        return out.cast(spec.schema)

    def _cells(self, sink: Sink, parquet: Path, where: str, audit: Audit) -> pa.Table:
        spec = TABLES["cfpb_complaints_daily"]
        book = self._codebooks.get(sink.name)
        if book is None:  # one read per run per sink; new codes go to the warehouse and to memory
            book = self._codebooks[sink.name] = CodeBook.load(sink)
        new_codes = book.assign_new(distinct_values(parquet, where), self.run_id)
        if new_codes.num_rows:
            sink.append_meta("cfpb_codes", new_codes)
            log.info("%s: %d new codes in cfpb_codes", sink.name, new_codes.num_rows)
        return encode_cells(parquet, where, book, _audit_sql(audit, spec.schema))

    def _tombstones(self, spec: TableSpec, existing_keys: pa.Table, file_rows: pa.Table, audit: Audit) -> pa.Table:
        """Keys the warehouse currently holds but the file no longer has -> tombstone rows."""
        if existing_keys is None or existing_keys.num_rows == 0:
            return spec.schema.empty_table()
        con = duckdb.connect()
        con.register("existing", existing_keys)
        con.register("file", file_rows)
        on = " AND ".join(f'e."{k}" = f."{k}"' for k in spec.key_columns)
        carry = [spec.window_column] if spec.window_column and spec.window_column in existing_keys.schema.names \
            and spec.window_column not in spec.key_columns else []
        keys = ", ".join(f'e."{k}"' for k in spec.key_columns + carry)
        gone = con.execute(f"SELECT DISTINCT {keys} FROM existing e ANTI JOIN file f ON {on}").to_arrow_table()
        con.close()
        n = gone.num_rows
        if n == 0:
            return spec.schema.empty_table()
        cols = {f.name: pa.nulls(n, f.type) for f in spec.schema}
        for k in spec.key_columns + carry:
            cols[k] = gone.column(k).cast(spec.schema.field(k).type)
        if "received_month_id" in spec.schema.names and "received_date" in gone.schema.names:
            d = gone.column("received_date").to_pylist()
            cols["received_month_id"] = pa.array([x.year * 100 + x.month for x in d], pa.int64())
        for f in spec.schema:
            if f.name in spec.key_columns or f.name in AUDIT_COLUMNS or f.nullable:
                continue
            cols[f.name] = pa.array([0] * n, f.type)  # NOT NULL measures (n_complaints) on tombstones
        cols["row_hash"] = pa.array(["tombstone"] * n)
        cols["_source"] = pa.array([audit.source] * n)
        cols["_source_as_of"] = pa.array([audit.as_of] * n, pa.date32())
        cols["_loaded_at"] = pa.array([audit.loaded_at] * n, pa.timestamp("us", tz="UTC"))
        if "_run_id" in spec.schema.names:
            cols["_run_id"] = pa.array([audit.run_id] * n)
        cols["_is_tombstone"] = pa.array([True] * n)
        return pa.table(cols, schema=spec.schema)

    # ------------------------------------------------------------------ windows / bookkeeping
    def _window(self, source: str, sink: Sink, mode: str, start: date | None, end: date | None, overlap_days: int,
                source_max: date) -> tuple[date, date]:
        if mode == "full_diff":
            return date.fromisoformat(self.settings.history_start), source_max
        if mode == "backfill":
            if not (start and end):
                raise ValueError("backfill needs --start and --end")
            return start, end
        state = sink.get_state(source)
        if state and state.watermark_date:
            s = state.watermark_date - timedelta(days=overlap_days)
        else:
            s = date.fromisoformat(self.settings.history_start)
        return s, max(source_max, s)

    def _finish(self, sink: Sink, source: str, mode: str, window, rows_read: int, rows_written: int, started: datetime,
                error: Exception | None, new_watermark: date | None, as_of: date | None) -> None:
        status = "failed" if error else "success"
        run = RunLog(self.run_id, source, sink.name, mode, window[0] if window else None, window[1] if window else None,
                     rows_read, rows_written, status, str(error)[:2000] if error else None, started, datetime.now(UTC))
        try:
            sink.log_run(run)
            if not error and new_watermark:
                prev = sink.get_state(source)
                wm = max(prev.watermark_date, new_watermark) if prev and prev.watermark_date else new_watermark
                sink.set_state(State(source, sink.name, wm, as_of, self.run_id, None))
        except Exception as exc:  # noqa: BLE001
            log.error("%s: could not write run log/state: %s", sink.name, exc)
        log.info("%s/%s %s window=%s read=%d written=%d status=%s", source, sink.name, mode, window, rows_read,
                 rows_written, status)

    # ------------------------------------------------------------------ ensure / rehydrate
    def ensure_tables(self) -> list[str]:
        notes: list[str] = []
        for sink in self.sinks:
            notes += sink.ensure_tables()
            if getattr(sink, "pending_rehydrate", None):
                notes += self.rehydrate(sink)
        for n in notes:
            log.info(n)
        return notes

    def _latest_cfpb_parquet(self) -> tuple[Path, date]:
        parquet = self.landing.latest("cfpb", "complaints", "parquet")
        if parquet is None:
            zip_path, as_of, _ = cfpb.download_bulk(self.settings, self.landing)
            parquet = self.landing.path("cfpb", "complaints", as_of, "parquet")
            cfpb.bulk_zip_to_parquet(zip_path, parquet)
        as_of = date.fromisoformat(parquet.stem.split("_")[-1])
        return parquet, as_of

    def _cfpb_rows(self, sink: Sink, parquet: Path, q0: date, q1: date, audit: Audit) -> tuple[TableSpec, pa.Table]:
        where = f"received_date BETWEEN DATE '{q0}' AND DATE '{q1}'"
        if _grain(sink) == "daily_cells":
            return TABLES["cfpb_complaints_daily"], self._cells(sink, parquet, where, audit)
        spec = TABLES["cfpb_complaints"]
        return spec, self._versions(parquet, spec, audit, where)

    def _load_cfpb_window(self, sink: Sink, parquet: Path, window: tuple[date, date], audit: Audit,
                          writer: str = "upsert_versions") -> tuple[int, int, int]:
        read = written = tombstones = 0
        for q0, q1 in _quarters(*window):
            spec, rows = self._cfpb_rows(sink, parquet, q0, q1, audit)
            read += rows.num_rows
            written += getattr(sink, writer)(spec, rows, (q0, q1)) if writer == "upsert_versions" else sink.rehydrate(spec, rows)  # type: ignore[attr-defined]
            if writer == "upsert_versions":
                tomb = self._tombstones(spec, sink.current_keys(spec, (q0, q1)), rows, audit)
                if tomb.num_rows:
                    written += sink.upsert_versions(spec, tomb, (q0, q1))
                    tombstones += tomb.num_rows
        return read, written, tombstones

    def rehydrate(self, sink: Sink) -> list[str]:
        """BigQuery after an expiry: full load of current versions into a fresh table."""
        if not hasattr(sink, "rehydrate"):
            return [f"{sink.name}: rehydrate not applicable"]
        parquet, as_of = self._latest_cfpb_parquet()
        audit = Audit("bulk_csv", as_of, self.run_id, datetime.now(UTC))
        started = datetime.now(UTC)
        window = (date.fromisoformat(self.settings.history_start), self.today)
        read = written = 0
        err: Exception | None = None
        try:
            read, written, _ = self._load_cfpb_window(sink, parquet, window, audit, writer="rehydrate")
        except Exception as exc:
            err = exc
            log.exception("%s: rehydrate failed", sink.name)
        self._finish(sink, "cfpb", "rehydrate", window, read, written, started, err, None if err else as_of, as_of)
        if err:
            raise SinkFailure(f"{sink.name}: rehydrate failed: {err}") from err
        return [f"{sink.name}: rehydrated with {written} rows from {parquet.name}"]

    # ------------------------------------------------------------------ CFPB
    def run_cfpb(self, mode: str = "incremental", start: date | None = None, end: date | None = None) -> None:
        zip_path, as_of, _downloaded = cfpb.download_bulk(self.settings, self.landing)
        parquet = self.landing.path("cfpb", "complaints", as_of, "parquet")
        if not parquet.exists():
            n = cfpb.bulk_zip_to_parquet(zip_path, parquet)
            log.info("normalised %d rows into %s", n, parquet.name)
            self.landing.prune("cfpb", "complaints", "parquet")
        con = duckdb.connect()
        file_max = con.execute(f"SELECT max(received_date) FROM read_parquet('{parquet.as_posix()}')").fetchone()[0]
        con.close()
        audit = Audit("bulk_csv", as_of, self.run_id, datetime.now(UTC))
        failures = []
        for sink in self.sinks:
            started = datetime.now(UTC)
            window = None
            read = written = 0
            err: Exception | None = None
            try:
                window = self._window("cfpb", sink, mode, start, end, self.settings.cfpb_overlap_days, file_max)
                read, written, tomb = self._load_cfpb_window(sink, parquet, window, audit)
                if tomb:
                    log.info("%s: %d tombstones", sink.name, tomb)
            except Exception as exc:
                err = exc
                log.exception("%s: cfpb %s failed", sink.name, mode)
            new_wm = None if err else (min(end, file_max) if mode == "backfill" and end else file_max)
            self._finish(sink, "cfpb", mode, window, read, written, started, err, new_wm, as_of)
            if err:
                failures.append(f"{sink.name}: {err}")
        if failures:
            raise SinkFailure("; ".join(failures))

    # ------------------------------------------------------------------ FDIC
    def run_fdic(self, mode: str = "incremental", **_) -> None:
        spec = TABLES["fdic_institutions"]
        audit = Audit("fdic_api", self.today, self.run_id, datetime.now(UTC))
        business = fdic.fetch_institutions(self.settings)
        if business.num_rows < 1000:
            raise RuntimeError(f"FDIC returned only {business.num_rows} institutions; refusing to treat that as truth")
        rows = self._versions(business, spec, audit)
        failures = []
        for sink in self.sinks:
            started = datetime.now(UTC)
            err = None
            written = 0
            try:
                written = sink.upsert_versions(spec, rows, None)
            except Exception as exc:
                err = exc
                log.exception("%s: fdic failed", sink.name)
            self._finish(sink, "fdic", "full", None, rows.num_rows, written, started, err, None if err else self.today, self.today)
            if err:
                failures.append(f"{sink.name}: {err}")
        if failures:
            raise SinkFailure("; ".join(failures))

    # ------------------------------------------------------------------ FRED
    def run_fred(self, mode: str = "incremental", start: date | None = None, end: date | None = None) -> None:
        spec = TABLES["fred_observations"]
        audit = Audit("fred_csv", self.today, self.run_id, datetime.now(UTC))
        failures = []
        cache: dict[tuple[date, date], pa.Table] = {}
        for sink in self.sinks:
            started = datetime.now(UTC)
            err = None
            read = written = 0
            window = None
            try:
                window = self._window("fred", sink, "incremental" if mode == "full_diff" else mode, start, end,
                                      self.settings.fred_overlap_days, self.today)
                if window not in cache:
                    cache[window] = fred.fetch_all(self.settings, *window)
                rows = self._versions(cache[window], spec, audit)
                read = rows.num_rows
                written = sink.upsert_versions(spec, rows, window)
            except Exception as exc:
                err = exc
                log.exception("%s: fred failed", sink.name)
            self._finish(sink, "fred", mode, window, read, written, started, err, None if err else (window[1] if window else None), self.today)
            if err:
                failures.append(f"{sink.name}: {err}")
        if failures:
            raise SinkFailure("; ".join(failures))

    # ------------------------------------------------------------------ status
    def status(self) -> str:
        lines = []
        for sink in self.sinks:
            lines.append(f"== {sink.name} (cfpb grain: {_grain(sink)})")
            try:
                for r in sink.query("SELECT * FROM {raw}._load_state ORDER BY source").to_pylist():
                    lines.append(f"  state  {r['source']:<5} watermark={r['watermark_date']} as_of={r['source_as_of']} run={r['last_run_id']}")
                runs = sink.query(
                    "SELECT source, mode, status, rows_read, rows_written, started_at, error FROM {raw}._load_runs "
                    "ORDER BY started_at DESC LIMIT 10"
                ).to_pylist()
                for r in runs:
                    lines.append(f"  run    {r['started_at']:%Y-%m-%d %H:%M} {r['source']:<5} {r['mode']:<11} {r['status']:<7} "
                                 f"read={r['rows_read']} written={r['rows_written']} {('ERR ' + r['error'][:80]) if r['error'] else ''}")
            except Exception as exc:  # noqa: BLE001
                lines.append(f"  (unavailable: {exc})")
        return "\n".join(lines)
