"""row_hash: first 32 hex chars of sha256 over the business columns.

Computed in DuckDB so 18M rows take seconds, with one canonical stringification:
dates as ISO 'YYYY-MM-DD', booleans as 'true'/'false', floats via DuckDB's shortest
round-trip text, nulls as '', fields joined by chr(31). The same expression is used for
every table and every warehouse, so a hash from today equals a hash from a backfill.
"""
from __future__ import annotations

import pyarrow as pa

SEP = "chr(31)"


def _canon(col: str, dtype: pa.DataType) -> str:
    q = f'"{col}"'
    if pa.types.is_date(dtype):
        return f"coalesce(strftime({q}, '%Y-%m-%d'), '')"
    if pa.types.is_boolean(dtype):
        return f"coalesce(case when {q} then 'true' else 'false' end, '')"
    return f"coalesce(cast({q} as varchar), '')"


def hash_expression(schema: pa.Schema, hash_columns: list[str], length: int = 32) -> str:
    parts = ", ".join(_canon(c, schema.field(c).type) for c in hash_columns)
    return f"substr(sha256(concat_ws({SEP}, {parts})), 1, {length})"
