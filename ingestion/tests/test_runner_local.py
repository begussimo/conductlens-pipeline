"""End-to-end against the DuckDB sink: idempotency, mutation, tombstones, watermarks."""
from datetime import date

import pytest
from conductlens_ingest.runner import Runner, SinkFailure, _quarters
from conductlens_ingest.schema import TABLES
from conductlens_ingest.sinks.local import LocalSink


def _runner(settings, run_id="t1"):
    sink = LocalSink(settings, path=":memory:")
    r = Runner(settings, sinks=[sink], run_id=run_id)
    r.ensure_tables()
    return r, sink


def _current(sink):
    return {r["complaint_id"]: r for r in sink.query(
        "SELECT * FROM (SELECT *, row_number() over (partition by complaint_id order by _loaded_at desc) rn "
        "FROM {raw}.cfpb_complaints) WHERE rn = 1").to_pylist()}


def test_first_run_then_rerun_writes_nothing(settings, fake_download, base_zip):
    fake_download(base_zip, date(2026, 10, 24))
    runner, sink = _runner(settings)
    runner.run_cfpb("incremental")
    runs = sink.query("SELECT * FROM {raw}._load_runs ORDER BY started_at").to_pylist()
    assert runs[-1]["status"] == "success"
    assert runs[-1]["rows_read"] == 5 and runs[-1]["rows_written"] == 5  # in-file duplicate collapsed
    state = sink.get_state("cfpb")
    assert state.watermark_date == date(2026, 10, 21)
    assert state.source_as_of == date(2026, 10, 24)

    Runner(settings, sinks=[sink], run_id="t2").run_cfpb("incremental")
    runs = sink.query("SELECT * FROM {raw}._load_runs ORDER BY started_at").to_pylist()
    assert runs[-1]["rows_written"] == 0
    assert runs[-1]["window_start"] == date(2026, 10, 21) - __import__("datetime").timedelta(days=60)
    assert sink.query("SELECT count(*) AS n FROM {raw}.cfpb_complaints").to_pylist()[0]["n"] == 5


def test_mutation_adds_a_version_and_staging_picks_latest(settings, fake_download, base_zip, mutated_zip):
    fake_download(base_zip, date(2026, 10, 24))
    runner, sink = _runner(settings)
    runner.run_cfpb("incremental")
    fake_download(mutated_zip, date(2026, 10, 25))
    Runner(settings, sinks=[sink], run_id="t2").run_cfpb("incremental")
    cur = _current(sink)
    assert cur[3]["company_response"] == "Closed with explanation" and cur[3]["timely_response"] == "Yes"
    assert cur[6]["received_date"] == date(2026, 10, 22)
    versions = sink.query("SELECT complaint_id, count(*) AS n FROM {raw}.cfpb_complaints GROUP BY 1").to_pylist()
    assert {v["complaint_id"]: v["n"] for v in versions}[3] == 2
    # complaint 4 (2025) is outside the 60-day window: still present, untouched, no tombstone
    assert cur[4]["_is_tombstone"] is False
    assert sink.get_state("cfpb").watermark_date == date(2026, 10, 22)


def test_full_diff_writes_tombstone_for_vanished_row(settings, fake_download, base_zip, mutated_zip):
    fake_download(base_zip, date(2026, 10, 24))
    runner, sink = _runner(settings)
    runner.run_cfpb("incremental")
    fake_download(mutated_zip, date(2026, 10, 25))
    Runner(settings, sinks=[sink], run_id="t2").run_cfpb("full_diff")
    cur = _current(sink)
    assert cur[4]["_is_tombstone"] is True and cur[4]["row_hash"] == "tombstone"
    spec = TABLES["cfpb_complaints"]
    keys = sink.current_keys(spec, None).to_pylist()
    assert {k["complaint_id"] for k in keys} == {1, 2, 3, 5, 6}
    # running full_diff again changes nothing
    Runner(settings, sinks=[sink], run_id="t3").run_cfpb("full_diff")
    last = sink.query("SELECT rows_written FROM {raw}._load_runs ORDER BY started_at DESC LIMIT 1").to_pylist()[0]
    assert last["rows_written"] == 0


def test_backfill_does_not_lower_watermark(settings, fake_download, base_zip):
    fake_download(base_zip, date(2026, 10, 24))
    runner, sink = _runner(settings)
    runner.run_cfpb("incremental")
    Runner(settings, sinks=[sink], run_id="bf").run_cfpb("backfill", start=date(2025, 1, 1), end=date(2025, 12, 31))
    assert sink.get_state("cfpb").watermark_date == date(2026, 10, 21)
    last = sink.query("SELECT * FROM {raw}._load_runs ORDER BY started_at DESC LIMIT 1").to_pylist()[0]
    assert last["mode"] == "backfill" and last["rows_read"] == 1 and last["rows_written"] == 0


def test_backfill_requires_dates(settings, fake_download, base_zip):
    fake_download(base_zip, date(2026, 10, 24))
    runner, sink = _runner(settings)
    with pytest.raises(SinkFailure):
        runner.run_cfpb("backfill")
    last = sink.query("SELECT status FROM {raw}._load_runs ORDER BY started_at DESC LIMIT 1").to_pylist()[0]
    assert last["status"] == "failed"


def test_quarter_chunks():
    qs = list(_quarters(date(2025, 11, 15), date(2026, 5, 3)))
    assert qs == [(date(2025, 11, 15), date(2025, 12, 31)), (date(2026, 1, 1), date(2026, 3, 31)),
                  (date(2026, 4, 1), date(2026, 5, 3))]
