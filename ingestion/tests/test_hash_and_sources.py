from datetime import date

import duckdb
import pyarrow as pa
from conductlens_ingest.hashing import hash_expression
from conductlens_ingest.schema import FRED_HASH_COLUMNS, FRED_SCHEMA, TABLES
from conductlens_ingest.sources import fdic, fred


def _hash(table: pa.Table, spec) -> list[str]:
    con = duckdb.connect()
    con.register("t", table)
    return [r[0] for r in con.execute(f"SELECT {hash_expression(spec.schema, spec.hash_columns)} FROM t").fetchall()]


def test_hash_is_stable_and_sensitive():
    spec = TABLES["fred_observations"]
    t = pa.table({"series_id": ["DGS10", "DGS10"], "observation_date": [date(2026, 1, 2), date(2026, 1, 2)],
                  "value": [4.5, 4.51]}, schema=pa.schema([FRED_SCHEMA.field(i) for i in range(3)]))
    h = _hash(t, spec)
    assert len(h[0]) == 32 and h[0] != h[1]
    assert _hash(t, spec) == h
    assert FRED_HASH_COLUMNS == ["series_id", "observation_date", "value"]


def test_fdic_record_parsing():
    r = fdic.record_to_row({"CERT": 10004, "NAME": "Ergo Bank", "DEP": 226157, "ASSET": "276713", "ACTIVE": "1",
                            "REPDTE": "06/30/2026", "DATEUPDT": "07/29/2026", "RSSDHCR": "1249691", "NAMEHCR": "FS BANCSHARES INC",
                            "ENDEFYMD": "", "STALP": "WI", "BKCLASS": "NM", "OFFICES": 6})
    assert r["cert"] == 10004 and r["dep_thousands"] == 226157 and r["asset_thousands"] == 276713
    assert r["active"] is True and r["repdte"] == date(2026, 6, 30) and r["endefymd"] is None
    assert r["rssdhcr"] == "1249691"
    assert fdic.record_to_row({"CERT": "1", "RSSDHCR": "0"})["rssdhcr"] is None


def test_fred_csv_parsing(monkeypatch, settings):
    class R:
        content = b"observation_date,DGS10\n2026-10-01,5.24\n2026-10-02,.\n2026-10-03,\n2026-10-05,5.31\n"

    monkeypatch.setattr(fred, "get_with_retry", lambda *a, **k: R())
    t = fred.fetch_series(settings, "DGS10", date(2026, 10, 1), date(2026, 10, 5))
    rows = t.to_pylist()
    assert [r["value"] for r in rows] == [5.24, None, None, 5.31]
    assert rows[0]["observation_date"] == date(2026, 10, 1) and rows[0]["series_id"] == "DGS10"
