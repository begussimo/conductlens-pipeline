from __future__ import annotations

from datetime import date

import duckdb
import pyarrow as pa

from ..schema import TableSpec


def anti_join_new_versions(spec: TableSpec, rows: pa.Table, existing: pa.Table) -> pa.Table:
    """Rows whose (keys, row_hash) are not in `existing`. Pure DuckDB, works at 20M rows."""
    if rows.num_rows == 0:
        return rows
    if existing.num_rows == 0:
        return rows
    con = duckdb.connect()
    con.register("rows", rows)
    con.register("existing", existing)
    on = " AND ".join(f'r."{k}" = e."{k}"' for k in spec.key_columns + ["row_hash"])
    out = con.execute(f"SELECT r.* FROM rows r ANTI JOIN existing e ON {on}").to_arrow_table()
    con.close()
    return out.cast(rows.schema)


def window_predicate(spec: TableSpec, window: tuple[date, date] | None, quote: str = '"') -> str:
    if window is None or spec.window_column is None:
        return "1=1"
    c = f"{quote}{spec.window_column}{quote}"
    return f"{c} >= DATE '{window[0].isoformat()}' AND {c} <= DATE '{window[1].isoformat()}'"


def current_version_sql(spec: TableSpec, fq_table: str, where: str, quote: str = '"') -> str:
    """Latest non-tombstone version per key, with the window column so tombstones can carry it."""
    keys = ", ".join(f"{quote}{k}{quote}" for k in spec.key_columns)
    extra = (f", {quote}{spec.window_column}{quote}"
             if spec.window_column and spec.window_column not in spec.key_columns else "")
    return f"""
        SELECT {keys}, {quote}row_hash{quote}{extra}
        FROM (
            SELECT {keys}, {quote}row_hash{quote}{extra}, {quote}_is_tombstone{quote},
                   ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {quote}_loaded_at{quote} DESC) AS rn
            FROM {fq_table}
            WHERE {where}
        ) v
        WHERE rn = 1 AND NOT {quote}_is_tombstone{quote}
    """
