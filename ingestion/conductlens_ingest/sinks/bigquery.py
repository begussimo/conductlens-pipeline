"""BigQuery sandbox sink.

Sandbox rules this code lives with (docs.cloud.google.com/bigquery/docs/sandbox, read 2026-10-24):
  * no DML: no MERGE / INSERT / UPDATE / DELETE statements. Everything here is a load job,
    a copy job, DDL, or a SELECT. Load and copy jobs are free.
  * every table expires 60 days after creation and the expiration cannot be removed.
    `ensure_tables` rotates any raw table that is within `bq_rotate_days_before_expiry` of
    dying: copy → drop → rename (one write), or, for the big complaints table when that fails,
    a fresh load from the latest landed parquet (`rehydrate`).
  * 10 GiB lifetime storage, 1 TiB/month query bytes. Writes are kept to new versions only.
"""
from __future__ import annotations

import io
import logging
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

from ..config import Settings
from ..schema import META_TABLES, TABLES, TableSpec, bq_schema
from ._common import anti_join_new_versions, current_version_sql, window_predicate
from .base import RunLog, State

log = logging.getLogger(__name__)


def _month_id(d: date) -> int:
    return d.year * 100 + d.month


class BigQuerySink:
    name = "bigquery"

    def __init__(self, settings: Settings):
        from google.cloud import bigquery

        if not settings.bq_project:
            raise RuntimeError("CL_BQ_PROJECT is not set")
        self.bq = bigquery
        self.settings = settings
        self.client = bigquery.Client(project=settings.bq_project, location=settings.bq_location)
        self.project = settings.bq_project
        self.raw = f"{self.project}.{settings.bq_raw_dataset}"
        self.marts = f"{self.project}.{settings.bq_prefix}_marts"
        self.rotate_before = timedelta(days=settings.bq_rotate_days_before_expiry)
        self.cfpb_grain = settings.bq_cfpb_grain  # daily_cells (default) | complaints
        self.pending_rehydrate: list[str] = []
        self._use_bqstorage = True

    # -------------------------------------------------------------- helpers
    def _fq(self, table: str) -> str:
        return f"{self.raw}.{table}"

    def _table(self, table: str):
        from google.api_core.exceptions import NotFound

        try:
            return self.client.get_table(self._fq(table))
        except NotFound:
            return None

    def table_exists(self, table: str) -> bool:
        return self._table(table) is not None

    def _job_config_query(self):
        return self.bq.QueryJobConfig(maximum_bytes_billed=20 * 1024**3, use_legacy_sql=False)

    def _run_query(self, sql: str) -> pa.Table:
        from google.api_core.exceptions import PermissionDenied

        job = self.client.query(sql, job_config=self._job_config_query())
        result = job.result()
        if self._use_bqstorage:
            try:
                return result.to_arrow(create_bqstorage_client=True)
            except PermissionDenied:
                # The service account lacks roles/bigquery.readSessionUser. Fall back to the REST
                # API for this process: slower for big key downloads, identical results.
                log.warning("BigQuery Storage Read API not permitted; falling back to REST row paging "
                            "(grant 'BigQuery Read Session User' to the service account to speed reads up)")
                self._use_bqstorage = False
                result = job.result()  # the failed iterator cannot be reused
        return result.to_arrow(create_bqstorage_client=False)

    def _load_arrow(self, table: str, rows: pa.Table, schema: pa.Schema, disposition: str) -> int:
        if rows.num_rows == 0 and disposition == "WRITE_APPEND":
            return 0
        buf = io.BytesIO()
        pq.write_table(rows.cast(schema), buf, compression="snappy")
        buf.seek(0)
        cfg = self.bq.LoadJobConfig(
            source_format=self.bq.SourceFormat.PARQUET,
            write_disposition=disposition,
            schema=bq_schema(schema),
        )
        job = self.client.load_table_from_file(buf, self._fq(table), job_config=cfg)
        job.result()
        return rows.num_rows

    def _create(self, name: str, schema: pa.Schema, spec: TableSpec | None = None, table_id: str | None = None):
        t = self.bq.Table(table_id or self._fq(name), schema=bq_schema(schema))
        if spec and spec.bq_partition:
            p = spec.bq_partition
            t.range_partitioning = self.bq.RangePartitioning(
                field=p["field"], range_=self.bq.PartitionRange(start=p["start"], end=p["end"], interval=p["interval"])
            )
        if spec and spec.bq_cluster:
            t.clustering_fields = spec.bq_cluster
        return self.client.create_table(t)

    # -------------------------------------------------------------- ddl / rotation
    def ensure_tables(self) -> list[str]:
        from google.api_core.exceptions import Conflict

        notes: list[str] = []
        ds = self.bq.Dataset(self.raw)
        ds.location = self.settings.bq_location
        try:
            self.client.create_dataset(ds)
            notes.append(f"bigquery: created dataset {self.raw}")
        except Conflict:
            pass
        now = datetime.now(UTC)
        for spec in TABLES.values():
            if self.name not in spec.sinks or spec.name == self._skipped_cfpb_table():
                continue  # only one CFPB grain lives on BigQuery (CL_BQ_CFPB_GRAIN)
            t = self._table(spec.name)
            if t is None:
                self._create(spec.name, spec.schema, spec)
                notes.append(f"bigquery: created {spec.name}")
                if spec.name.startswith("cfpb_complaints"):
                    self.pending_rehydrate.append(spec.name)
                    notes.append(f"bigquery: {spec.name} is empty -> rehydrate required")
            elif t.expires is not None and t.expires - now <= self.rotate_before:
                notes.append(self._rotate(spec.name, spec.schema, spec, t.expires))
        for name, schema in META_TABLES.items():
            t = self._table(name)
            if t is None:
                self._create(name, schema)
                notes.append(f"bigquery: created {name}")
            elif t.expires is not None and t.expires - now <= self.rotate_before:
                notes.append(self._rotate(name, schema, None, t.expires))
        return notes

    def _skipped_cfpb_table(self) -> str:
        return "cfpb_complaints" if self.cfpb_grain == "daily_cells" else "cfpb_complaints_daily"

    def _rotate(self, name: str, schema: pa.Schema, spec: TableSpec | None, expires: datetime) -> str:
        """copy → drop → rename keeps content and history in one write. If the rename is refused,
        the big table is dropped empty and flagged for `rehydrate`; small ones are reloaded from
        the copy we just made."""
        tmp = f"{self._fq(name)}__rotating"
        self.client.delete_table(tmp, not_found_ok=True)
        self._create(name, schema, spec, table_id=tmp)
        copy = self.client.copy_table(
            self._fq(name), tmp, job_config=self.bq.CopyJobConfig(write_disposition="WRITE_TRUNCATE")
        )
        copy.result()
        self.client.delete_table(self._fq(name))
        try:
            self.client.query(f"ALTER TABLE `{tmp}` RENAME TO `{name}`", job_config=self._job_config_query()).result()
            return f"bigquery: rotated {name} (was expiring {expires:%Y-%m-%d}); history kept"
        except Exception as exc:  # noqa: BLE001 - sandbox may refuse; fall back explicitly
            log.warning("rename refused for %s (%s); falling back", name, exc)
            self._create(name, schema, spec)
            if name.startswith("cfpb_complaints"):
                self.client.delete_table(tmp, not_found_ok=True)
                self.pending_rehydrate.append(name)
                return f"bigquery: recreated {name} empty -> rehydrate required"
            rows = self.client.list_rows(tmp).to_arrow()
            self._load_arrow(name, rows, schema, "WRITE_TRUNCATE")
            self.client.delete_table(tmp, not_found_ok=True)
            return f"bigquery: rotated {name} via reload ({rows.num_rows} rows)"

    def rehydrate(self, spec: TableSpec, rows: pa.Table) -> int:
        """Full load of current versions into an (empty, fresh) table."""
        if not self.table_exists(spec.name):
            self._create(spec.name, spec.schema, spec)
        n = self._load_arrow(spec.name, rows, spec.schema, "WRITE_APPEND")
        self.pending_rehydrate = [t for t in self.pending_rehydrate if t != spec.name]
        return n

    # -------------------------------------------------------------- data
    def _window_sql(self, spec: TableSpec, window) -> str:
        pred = window_predicate(spec, window, quote="`")
        if window and spec.bq_partition and spec.name.startswith("cfpb_complaints"):
            pred += f" AND `received_month_id` BETWEEN {_month_id(window[0])} AND {_month_id(window[1])}"
        return pred

    def current_keys(self, spec: TableSpec, window: tuple[date, date] | None) -> pa.Table:
        sql = current_version_sql(spec, f"`{self._fq(spec.name)}`", self._window_sql(spec, window), quote="`")
        return self._run_query(sql)

    def _all_version_keys(self, spec: TableSpec, window) -> pa.Table:
        keys = ", ".join(f"`{k}`" for k in spec.key_columns)
        return self._run_query(
            f"SELECT {keys}, `row_hash` FROM `{self._fq(spec.name)}` WHERE {self._window_sql(spec, window)}"
        )

    def upsert_versions(self, spec: TableSpec, rows: pa.Table, window: tuple[date, date] | None) -> int:
        existing = self._all_version_keys(spec, window) if self.table_exists(spec.name) else spec.schema.empty_table()
        new = anti_join_new_versions(spec, rows, existing)
        return self._load_arrow(spec.name, new, spec.schema, "WRITE_APPEND")

    # -------------------------------------------------------------- meta
    def get_state(self, source: str) -> State | None:
        if not self.table_exists("_load_state"):
            return None
        t = self._run_query(
            f"SELECT * FROM `{self._fq('_load_state')}` WHERE source = '{source}' AND sink = '{self.name}'"
        )
        if t.num_rows == 0:
            return None
        r = t.to_pylist()[0]
        return State(r["source"], r["sink"], r["watermark_date"], r["source_as_of"], r["last_run_id"], r["updated_at"])

    def set_state(self, state: State) -> None:
        schema = META_TABLES["_load_state"]
        current = self._run_query(f"SELECT * FROM `{self._fq('_load_state')}`").to_pylist() if self.table_exists("_load_state") else []
        kept = [r for r in current if not (r["source"] == state.source and r["sink"] == self.name)]
        kept.append({
            "source": state.source, "sink": self.name, "watermark_date": state.watermark_date,
            "source_as_of": state.source_as_of, "last_run_id": state.last_run_id, "updated_at": datetime.now(UTC),
        })
        self._load_arrow("_load_state", pa.Table.from_pylist(kept, schema=schema), schema, "WRITE_TRUNCATE")

    def log_run(self, run: RunLog) -> None:
        schema = META_TABLES["_load_runs"]
        row = {**run.__dict__, "sink": self.name}
        self._load_arrow("_load_runs", pa.Table.from_pylist([row], schema=schema), schema, "WRITE_APPEND")

    def append_meta(self, table: str, rows: pa.Table) -> None:
        self._load_arrow(table, rows, META_TABLES[table], "WRITE_APPEND")

    def query(self, sql: str) -> pa.Table:
        return self._run_query(sql.format(raw=f"`{self.raw}`", marts=f"`{self.marts}`"))
