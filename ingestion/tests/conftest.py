import csv
import io
import zipfile
from datetime import date
from pathlib import Path

import pytest

HEADER = ["Date received", "Product", "Sub-product", "Issue", "Sub-issue", "Company public response", "Company",
          "State", "ZIP code", "Tags", "Submitted via", "Date sent to company", "Company response to consumer",
          "Timely response?", "Complaint ID"]


def row(cid, received, company="JPMORGAN CHASE & CO.", product="Credit card", response="In progress", timely="",
        sent=None, state="NY", sub_product="General-purpose credit card or charge card", issue="Fees or interest"):
    return [received, product, sub_product, issue, "Problem with fees", "", company, state, "100XX", "", "Web",
            sent if sent is not None else received, response, timely, str(cid)]


BASE_ROWS = [
    row(1, "2026-10-01", response="Closed with explanation", timely="Yes"),
    row(2, "2026-10-02", company="Chime Financial Inc", product="Checking or savings account",
        response="Closed with monetary relief", timely="Yes"),
    row(3, "2026-10-20"),                                   # still in progress: will mutate later
    row(4, "2025-06-15", response="Closed with non-monetary relief", timely="Yes"),  # old row
    row(5, "2026-10-21", company="Coinbase, Inc.", product="Money transfer, virtual currency, or money service",
        sub_product="Virtual currency", sent=""),
    row(5, "2026-10-21", company="Coinbase, Inc.", product="Money transfer, virtual currency, or money service",
        sub_product="Virtual currency", sent=""),            # exact duplicate line inside the file
]


def make_zip(path: Path, rows) -> Path:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(HEADER)
    w.writerows(rows)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("complaints.csv", buf.getvalue())
    return path


@pytest.fixture
def base_zip(tmp_path) -> Path:
    return make_zip(tmp_path / "complaints_2026-10-24.csv.zip", BASE_ROWS)


@pytest.fixture
def mutated_zip(tmp_path) -> Path:
    rows = [r for r in BASE_ROWS if r[-1] != "4"]            # complaint 4 disappears
    rows = [row(3, "2026-10-20", response="Closed with explanation", timely="Yes") if r[-1] == "3" else r for r in rows]
    rows.append(row(6, "2026-10-22"))                         # brand new complaint
    return make_zip(tmp_path / "complaints_2026-10-25.csv.zip", rows)


@pytest.fixture
def settings(tmp_path, monkeypatch):
    monkeypatch.setenv("CL_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("CL_SINKS", "local")
    monkeypatch.setenv("CL_LOCAL_DB", str(tmp_path / "local.duckdb"))
    monkeypatch.setenv("CL_HISTORY_START", "2011-12-01")
    from conductlens_ingest.config import Settings

    return Settings()


@pytest.fixture
def fake_download(monkeypatch):
    """Make cfpb.download_bulk hand back a prepared zip instead of touching the network."""
    state = {}

    def set_zip(path: Path, as_of: date):
        state["path"], state["as_of"] = path, as_of

    def _download(settings, landing):
        target = landing.path("cfpb", "complaints", state["as_of"], "csv.zip")
        if not target.exists():
            target.write_bytes(Path(state["path"]).read_bytes())
        return target, state["as_of"], True

    from conductlens_ingest.sources import cfpb

    monkeypatch.setattr(cfpb, "download_bulk", _download)
    return set_zip
