"""Databricks Free Edition sink: parquet → Unity Catalog volume → MERGE … WHEN NOT MATCHED.

Free Edition gives one serverless SQL warehouse (2X-Small), Unity Catalog with managed
volumes, and no expiry. MERGE on (keys, row_hash) is the idempotency guarantee; the
uploaded file is deleted after the merge.
"""
from __future__ import annotations

import io
import logging
import uuid
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

from ..config import Settings
from ..schema import META_TABLES, TABLES, TableSpec, dbx_columns_ddl
from ._common import current_version_sql, window_predicate
from .base import RunLog, State

log = logging.getLogger(__name__)


class DatabricksSink:
    name = "databricks"

    def __init__(self, settings: Settings):
        from databricks import sql as dbsql
        from databricks.sdk import WorkspaceClient

        if not settings.dbx_http_path:
            raise RuntimeError("DATABRICKS_HTTP_PATH is not set")
        self.settings = settings
        self.w = WorkspaceClient()  # host/token/profile resolved by the SDK; never read here
        host = self.w.config.host.replace("https://", "").replace("http://", "").rstrip("/")
        kwargs = {"server_hostname": host, "http_path": settings.dbx_http_path}
        if self.w.config.token:
            kwargs["access_token"] = self.w.config.token
        else:
            kwargs["credentials_provider"] = lambda: self.w.config.authenticate
        self.conn = dbsql.connect(**kwargs)
        self.catalog = settings.dbx_catalog
        self.raw = f"`{self.catalog}`.`{settings.dbx_raw_schema}`"
        self.marts = f"`{self.catalog}`.`{settings.dbx_prefix}_marts`"
        self.volume_path = f"/Volumes/{self.catalog}/{settings.dbx_raw_schema}/{settings.dbx_volume}"

    # -------------------------------------------------------------- helpers
    def _exec(self, sql: str, params: dict | None = None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            try:
                return cur.fetchall_arrow()
            except Exception:  # noqa: BLE001 - statements without a result set
                return None

    def _fq(self, table: str) -> str:
        return f"{self.raw}.`{table}`"

    def table_exists(self, table: str) -> bool:
        t = self._exec(
            f"SELECT count(*) AS n FROM `{self.catalog}`.information_schema.tables "
            f"WHERE table_schema = '{self.settings.dbx_raw_schema}' AND table_name = '{table}'"
        )
        return bool(t and t.column(0)[0].as_py())

    # -------------------------------------------------------------- ddl
    def ensure_tables(self) -> list[str]:
        notes = []
        p = self.settings.dbx_prefix
        for s in ("raw", "staging", "marts", "snapshots"):
            self._exec(f"CREATE SCHEMA IF NOT EXISTS `{self.catalog}`.`{p}_{s}`")
        self._exec(f"CREATE VOLUME IF NOT EXISTS {self.raw}.`{self.settings.dbx_volume}`")
        notes.append(f"databricks: ensured schemas {p}_* and volume {self.volume_path}")
        for spec in TABLES.values():
            if self.name not in spec.sinks:
                continue
            cluster = ", ".join(f"`{c}`" for c in spec.dbx_cluster)
            self._exec(
                f"CREATE TABLE IF NOT EXISTS {self._fq(spec.name)} ({dbx_columns_ddl(spec.schema)}) "
                f"USING DELTA CLUSTER BY ({cluster})"
            )
            notes.append(f"databricks: ensured {spec.name}")
        for name, schema in META_TABLES.items():
            self._exec(f"CREATE TABLE IF NOT EXISTS {self._fq(name)} ({dbx_columns_ddl(schema)}) USING DELTA")
        return notes

    # -------------------------------------------------------------- data
    def _upload(self, rows: pa.Table, schema: pa.Schema, subdir: str) -> str:
        buf = io.BytesIO()
        pq.write_table(rows.cast(schema), buf, compression="snappy")
        buf.seek(0)
        path = f"{self.volume_path}/{subdir}/{uuid.uuid4().hex}.parquet"
        self.w.files.upload(path, buf, overwrite=True)
        return path

    def _delete(self, path: str) -> None:
        try:
            self.w.files.delete(path)
        except Exception as exc:  # noqa: BLE001
            log.warning("could not delete %s: %s", path, exc)

    def upsert_versions(self, spec: TableSpec, rows: pa.Table, window: tuple[date, date] | None) -> int:
        if rows.num_rows == 0:
            return 0
        path = self._upload(rows, spec.schema, spec.name)
        try:
            on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in spec.key_columns + ["row_hash"])
            cols = ", ".join(f"`{f.name}`" for f in spec.schema)
            # Deduplicate the incoming batch first: MERGE refuses multiple source rows per target row.
            keys = ", ".join(f"`{k}`" for k in spec.key_columns + ["row_hash"])
            result = self._exec(
                f"""MERGE INTO {self._fq(spec.name)} AS t
                    USING (SELECT {cols} FROM (
                              SELECT *, ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY `_loaded_at` DESC) AS rn
                              FROM parquet.`{path}`) WHERE rn = 1) AS s
                    ON {on}
                    WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({', '.join('s.' + c for c in cols.split(', '))})"""
            )
        finally:
            self._delete(path)
        if result is not None and result.num_rows and "num_inserted_rows" in result.schema.names:
            return int(result.column("num_inserted_rows")[0].as_py() or 0)
        return rows.num_rows

    def current_keys(self, spec: TableSpec, window: tuple[date, date] | None) -> pa.Table:
        sql = current_version_sql(spec, self._fq(spec.name), window_predicate(spec, window, quote="`"), quote="`")
        return self._exec(sql)

    # -------------------------------------------------------------- meta
    def get_state(self, source: str) -> State | None:
        t = self._exec(
            f"SELECT source, sink, watermark_date, source_as_of, last_run_id, updated_at FROM {self._fq('_load_state')} "
            f"WHERE source = :source AND sink = :sink", {"source": source, "sink": self.name}
        )
        if t is None or t.num_rows == 0:
            return None
        r = t.to_pylist()[0]
        return State(r["source"], r["sink"], r["watermark_date"], r["source_as_of"], r["last_run_id"], r["updated_at"])

    def set_state(self, state: State) -> None:
        self._exec(
            f"""MERGE INTO {self._fq('_load_state')} AS t
                USING (SELECT :source AS source, :sink AS sink, CAST(:wm AS DATE) AS watermark_date,
                              CAST(:as_of AS DATE) AS source_as_of, :run_id AS last_run_id,
                              current_timestamp() AS updated_at) AS s
                ON t.source = s.source AND t.sink = s.sink
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *""",
            {"source": state.source, "sink": self.name, "wm": state.watermark_date, "as_of": state.source_as_of,
             "run_id": state.last_run_id},
        )

    def log_run(self, run: RunLog) -> None:
        self._exec(
            f"""INSERT INTO {self._fq('_load_runs')} VALUES (:run_id, :source, :sink, :mode, :ws, :we, :rr, :rw,
                :status, :error, :started, :finished)""",
            {"run_id": run.run_id, "source": run.source, "sink": self.name, "mode": run.mode, "ws": run.window_start,
             "we": run.window_end, "rr": run.rows_read, "rw": run.rows_written, "status": run.status,
             "error": (run.error or None), "started": run.started_at, "finished": run.finished_at},
        )

    def append_meta(self, table: str, rows: pa.Table) -> None:
        if rows.num_rows == 0:
            return
        path = self._upload(rows, META_TABLES[table], table)
        try:
            self._exec(f"INSERT INTO {self._fq(table)} SELECT * FROM parquet.`{path}`")
        finally:
            self._delete(path)

    def query(self, sql: str) -> pa.Table:
        return self._exec(sql.format(raw=self.raw, marts=self.marts))

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001
            pass
