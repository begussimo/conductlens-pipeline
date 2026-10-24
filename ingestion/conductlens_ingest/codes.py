"""Integer codes for the BigQuery daily-cell grain (ARCHITECTURE §3.1b).

A CodeBook is the `cfpb_codes` table in memory: (dimension, code, value). New values get
max(code)+1 within their dimension and are appended to the warehouse before the cells that use
them are written, so a cell row can never reference a code that does not exist. Code 0 is
reserved for NULL and never stored in the dictionary.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa

from .schema import CFPB_CODES_SCHEMA, CFPB_DAILY_CODE_DIMENSIONS, CFPB_DAILY_SCHEMA


class CodeBook:
    def __init__(self, rows: pa.Table | None = None):
        self.codes: dict[tuple[str, str], int] = {}
        self.max_code: dict[str, int] = {d: 0 for d in CFPB_DAILY_CODE_DIMENSIONS}
        if rows is not None:
            for r in rows.to_pylist():
                self.codes[(r["dimension"], r["value"])] = int(r["code"])
                self.max_code[r["dimension"]] = max(self.max_code.get(r["dimension"], 0), int(r["code"]))

    @classmethod
    def load(cls, sink) -> CodeBook:
        try:
            rows = sink.query("SELECT dimension, code, value FROM {raw}.cfpb_codes")
        except Exception:  # noqa: BLE001 - table may not exist yet
            rows = None
        return cls(rows)

    def assign_new(self, values: dict[str, list[str]], run_id: str) -> pa.Table:
        """Give codes to unseen values; return the rows to append to cfpb_codes."""
        now = datetime.now(UTC)
        new = []
        for dim, vals in values.items():
            for v in sorted(x for x in set(vals) if x is not None):
                if (dim, v) not in self.codes:
                    self.max_code[dim] = self.max_code.get(dim, 0) + 1
                    self.codes[(dim, v)] = self.max_code[dim]
                    new.append({"dimension": dim, "code": self.max_code[dim], "value": v, "_loaded_at": now, "_run_id": run_id})
        return pa.Table.from_pylist(new, schema=CFPB_CODES_SCHEMA)

    def as_table(self) -> pa.Table:
        return pa.table({
            "dimension": pa.array([d for d, _ in self.codes], pa.string()),
            "value": pa.array([v for _, v in self.codes], pa.string()),
            "code": pa.array(list(self.codes.values()), pa.int64()),
        })


def distinct_values(parquet: Path, where: str) -> dict[str, list[str]]:
    con = duckdb.connect()
    out = {}
    for d in CFPB_DAILY_CODE_DIMENSIONS:
        out[d] = [r[0] for r in con.execute(
            f"SELECT DISTINCT \"{d}\" FROM read_parquet('{parquet.as_posix()}') WHERE {where} AND \"{d}\" IS NOT NULL"
        ).fetchall()]
    con.close()
    return out


def encode_cells(parquet: Path, where: str, book: CodeBook, audit_sql: str) -> pa.Table:
    """Aggregate complaint rows in `where` to daily cells with integer codes and audit columns."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.register("codes", book.as_table())
    joins, selects = [], []
    for d in CFPB_DAILY_CODE_DIMENSIONS:
        joins.append(f'LEFT JOIN codes c_{d} ON c_{d}.dimension = \'{d}\' AND c_{d}.value = r."{d}"')
        selects.append(f'coalesce(c_{d}.code, 0) AS "{d}_code"')
    sql = f"""
        WITH r AS (  -- one row per complaint, even if the file repeats a line
            SELECT * FROM read_parquet('{parquet.as_posix()}') WHERE {where}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY complaint_id ORDER BY 1) = 1
        ),
        coded AS (
            SELECT r.received_date, r.received_month_id, {', '.join(selects)}
            FROM r
            {' '.join(joins)}
        ),
        cells AS (
            SELECT received_date, received_month_id, {', '.join(f'"{d}_code"' for d in CFPB_DAILY_CODE_DIMENSIONS)},
                   count(*) AS n_complaints
            FROM coded GROUP BY ALL
        )
        SELECT cells.*, substr(sha256(cast(n_complaints AS varchar)), 1, 16) AS row_hash, {audit_sql}
        FROM cells
    """
    out = con.execute(sql).to_arrow_table()
    con.close()
    return out.cast(CFPB_DAILY_SCHEMA)
