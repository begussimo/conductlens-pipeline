"""FRED series via fredgraph.csv (no API key). '.' means no observation and becomes null.

fred.stlouisfed.org silently drops connections for unknown User-Agent strings (tested
2026-10-24: curl, Wget and python-requests defaults pass, anything custom times out), so this
module deliberately uses a plain requests session with the library's default User-Agent."""
from __future__ import annotations

import io
import logging
from datetime import date

import pyarrow as pa
import pyarrow.csv as pacsv
import requests

from ..config import Settings
from ..http import get_with_retry
from ..schema import FRED_BUSINESS_FIELDS

log = logging.getLogger(__name__)
BUSINESS_SCHEMA = pa.schema(FRED_BUSINESS_FIELDS)
URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"


def fetch_series(settings: Settings, series_id: str, start: date, end: date) -> pa.Table:
    s = requests.Session()  # default UA on purpose, see module docstring
    r = get_with_retry(s, URL, params={"id": series_id, "cosd": start.isoformat(), "coed": end.isoformat()},
                       timeout=settings.request_timeout_s, max_attempts=settings.max_attempts)
    text = r.content
    if not text.startswith(b"observation_date") and not text.startswith(b"DATE"):
        raise ValueError(f"unexpected FRED response for {series_id}: {text[:60]!r}")
    table = pacsv.read_csv(
        io.BytesIO(text),
        convert_options=pacsv.ConvertOptions(column_types={series_id: pa.string()}, null_values=[".", ""], strings_can_be_null=True),
    )
    date_col = table.column(0).cast(pa.date32())
    values = table.column(series_id).cast(pa.float64())
    out = pa.table(
        {"series_id": pa.array([series_id] * table.num_rows, pa.string()), "observation_date": date_col, "value": values},
        schema=BUSINESS_SCHEMA,
    )
    log.info("fred %s: %d observations %s..%s", series_id, out.num_rows, start, end)
    return out


def fetch_all(settings: Settings, start: date, end: date) -> pa.Table:
    tables = [fetch_series(settings, sid, start, end) for sid in settings.fred_series]
    return pa.concat_tables(tables) if tables else BUSINESS_SCHEMA.empty_table()
