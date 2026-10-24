"""The BigQuery grain: daily cells with integer codes, exercised through the DuckDB sink."""
from datetime import date

from conductlens_ingest.runner import Runner
from conductlens_ingest.schema import CFPB_DAILY_KEYS, TABLES
from conductlens_ingest.sinks.local import LocalSink


def _runner(settings, run_id="c1"):
    sink = LocalSink(settings, path=":memory:", cfpb_grain="daily_cells")
    r = Runner(settings, sinks=[sink], run_id=run_id)
    r.ensure_tables()
    return r, sink


def _current_cells(sink):
    keys = ", ".join(CFPB_DAILY_KEYS)
    return sink.query(
        f"SELECT * FROM (SELECT *, row_number() over (partition by {keys} order by _loaded_at desc) rn "
        "FROM {raw}.cfpb_complaints_daily) WHERE rn = 1 AND NOT _is_tombstone").to_pylist()


def _decode(sink, dim, code):
    if code == 0:
        return None
    return sink.query(f"SELECT value FROM {{raw}}.cfpb_codes WHERE dimension='{dim}' AND code={code}").to_pylist()[0]["value"]


def test_cells_codes_and_counts(settings, fake_download, base_zip):
    fake_download(base_zip, date(2026, 10, 24))
    runner, sink = _runner(settings)
    runner.run_cfpb("incremental")
    cells = _current_cells(sink)
    assert sum(c["n_complaints"] for c in cells) == 5                # duplicate line collapsed
    assert sink.query("SELECT count(*) AS n FROM {raw}.cfpb_complaints").to_pylist()[0]["n"] == 0  # not landed at this grain
    codes = sink.query("SELECT dimension, count(*) AS n FROM {raw}.cfpb_codes GROUP BY 1 ORDER BY 1").to_pylist()
    dims = {c["dimension"]: c["n"] for c in codes}
    assert dims["company"] == 3 and dims["timely_response"] == 1 and "consumer_disputed" not in dims  # all-null dim -> no codes
    coinbase = [c for c in cells if _decode(sink, "company", c["company_code"]) == "Coinbase, Inc."][0]
    assert coinbase["timely_response_code"] == 0 and coinbase["consumer_disputed_code"] == 0
    assert _decode(sink, "sub_product", coinbase["sub_product_code"]) == "Virtual currency"
    assert coinbase["received_month_id"] == 202610 and len(coinbase["row_hash"]) == 16
    assert "_run_id" not in coinbase


def test_cell_mutation_versions_and_tombstones(settings, fake_download, base_zip, mutated_zip):
    fake_download(base_zip, date(2026, 10, 24))
    runner, sink = _runner(settings)
    runner.run_cfpb("incremental")
    n_codes_before = sink.query("SELECT count(*) AS n FROM {raw}.cfpb_codes").to_pylist()[0]["n"]
    fake_download(mutated_zip, date(2026, 10, 25))
    Runner(settings, sinks=[sink], run_id="c2").run_cfpb("incremental")
    cells = _current_cells(sink)
    # complaint 3 moved from (In progress, null timely) to (Closed with explanation, Yes):
    # the old cell got a tombstone, the new cell a version; complaint 6 is a brand-new cell
    by_resp = {}
    for c in cells:
        by_resp.setdefault(_decode(sink, "company_response", c["company_response_code"]), 0)
        by_resp[_decode(sink, "company_response", c["company_response_code"])] += c["n_complaints"]
    assert by_resp["Closed with explanation"] == 2 and by_resp["In progress"] == 2  # ids 1,3 explanation; 5,6 in progress
    tomb = sink.query("SELECT count(*) AS n FROM {raw}.cfpb_complaints_daily WHERE _is_tombstone").to_pylist()[0]["n"]
    assert tomb == 1
    # complaint 4 (2025-06) is outside the 60-day window: its cell is untouched
    assert any(c["received_date"] == date(2025, 6, 15) for c in cells)
    # codes only grow; the dictionary never rewrites existing codes
    assert sink.query("SELECT count(*) AS n FROM {raw}.cfpb_codes").to_pylist()[0]["n"] >= n_codes_before
    # re-run: nothing new
    Runner(settings, sinks=[sink], run_id="c3").run_cfpb("incremental")
    last = sink.query("SELECT rows_written FROM {raw}._load_runs ORDER BY started_at DESC LIMIT 1").to_pylist()[0]
    assert last["rows_written"] == 0
    # full_diff tombstones the 2025 cell of vanished complaint 4
    Runner(settings, sinks=[sink], run_id="c4").run_cfpb("full_diff")
    assert not any(c["received_date"] == date(2025, 6, 15) for c in _current_cells(sink))
    spec = TABLES["cfpb_complaints_daily"]
    assert sink.current_keys(spec, None).num_rows == len(_current_cells(sink))
