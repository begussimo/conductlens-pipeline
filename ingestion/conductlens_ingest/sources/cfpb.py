"""CFPB Consumer Complaint Database.

Primary path: the daily bulk CSV (files.consumerfinance.gov/ccdb/complaints.csv.zip, ~360 MB
zipped, ~5.6 GB raw, ~18.7M rows, refreshed around 09:00 UTC). The JSON search API is
geo-fenced (HTTP 403 outside the US), so it is an optional second path used only to fill the
API-only columns (consumer_disputed, consumer_consent_provided, has_narrative).

Narratives are never requested, never written: the API client asks for `field=` without
complaint_what_happened and the bulk file does not carry them.
"""
from __future__ import annotations

import json
import logging
import zipfile
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from ..config import Settings
from ..http import SourceError, get_with_retry, session
from ..landing import Landing
from ..schema import CFPB_BUSINESS_FIELDS

log = logging.getLogger(__name__)

CSV_TO_COLUMN = {
    "Date received": "received_date",
    "Product": "product",
    "Sub-product": "sub_product",
    "Issue": "issue",
    "Sub-issue": "sub_issue",
    "Company public response": "company_public_response",
    "Company": "company",
    "State": "state",
    "ZIP code": "zip_code",
    "Tags": "tags",
    "Submitted via": "submitted_via",
    "Date sent to company": "date_sent_to_company",
    "Company response to consumer": "company_response",
    "Timely response?": "timely_response",
    "Complaint ID": "complaint_id",
    # present in older exports / the API; harmless if absent
    "Consumer disputed?": "consumer_disputed",
    "Consumer consent provided?": "consumer_consent_provided",
}
API_TO_COLUMN = {
    "date_received": "received_date", "product": "product", "sub_product": "sub_product", "issue": "issue",
    "sub_issue": "sub_issue", "company_public_response": "company_public_response", "company": "company",
    "state": "state", "zip_code": "zip_code", "tags": "tags", "submitted_via": "submitted_via",
    "date_sent_to_company": "date_sent_to_company", "company_response": "company_response", "timely": "timely_response",
    "consumer_disputed": "consumer_disputed", "consumer_consent_provided": "consumer_consent_provided",
    "complaint_id": "complaint_id", "has_narrative": "has_narrative",
}
BUSINESS_SCHEMA = pa.schema(CFPB_BUSINESS_FIELDS)


# ------------------------------------------------------------------ download
def download_bulk(settings: Settings, landing: Landing) -> tuple[Path, date, bool]:
    """Download the bulk zip unless the server's copy is unchanged (ETag / If-Modified-Since).

    Returns (zip_path, as_of_date, downloaded_now). as_of is the file's Last-Modified date in UTC.
    """
    s = session()
    etag_file = landing.dir("cfpb") / "complaints.etag"
    headers = {}
    latest_zip = landing.latest("cfpb", "complaints", "csv.zip")
    if etag_file.exists() and latest_zip is not None:
        try:
            saved = json.loads(etag_file.read_text())
            headers["If-None-Match"] = saved["etag"]
        except (ValueError, KeyError):
            pass
    r = get_with_retry(s, settings.cfpb_bulk_url, headers=headers, timeout=settings.request_timeout_s,
                       max_attempts=settings.max_attempts, stream=True)
    if r.status_code == 304 and latest_zip is not None:
        as_of = date.fromisoformat(latest_zip.stem.split("_")[-1].replace(".csv", ""))
        log.info("bulk file unchanged on server (304); reusing %s", latest_zip.name)
        return latest_zip, as_of, False
    lm = r.headers.get("Last-Modified")
    as_of = parsedate_to_datetime(lm).astimezone(UTC).date() if lm else datetime.now(UTC).date()
    target = landing.path("cfpb", "complaints", as_of, "csv.zip")
    tmp = target.with_suffix(".part")
    size = 0
    with open(tmp, "wb") as fh:
        for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
            fh.write(chunk)
            size += len(chunk)
    expected = int(r.headers.get("Content-Length") or 0)
    if expected and size != expected:
        tmp.unlink(missing_ok=True)
        raise SourceError(f"bulk download truncated: {size} of {expected} bytes")
    tmp.replace(target)
    if r.headers.get("ETag"):
        etag_file.write_text(json.dumps({"etag": r.headers["ETag"], "as_of": as_of.isoformat()}))
    log.info("downloaded %s (%.0f MB)", target.name, size / 1e6)
    landing.prune("cfpb", "complaints", "csv.zip")
    return target, as_of, True


# ------------------------------------------------------------------ normalise
def _to_date(arr: pa.Array) -> pa.Array:
    s = pc.utf8_trim_whitespace(arr.cast(pa.string()))
    s = pc.if_else(pc.equal(s, ""), pa.nulls(len(s), pa.string()), s)
    # Accept both 'YYYY-MM-DD' (bulk file) and 'MM/DD/YYYY' (older exports).
    iso = pc.strptime(pc.if_else(pc.match_substring_regex(s, r"^\d{4}-\d{2}-\d{2}"), pc.utf8_slice_codeunits(s, 0, 10), None),
                      format="%Y-%m-%d", unit="s", error_is_null=True)
    us = pc.strptime(pc.if_else(pc.match_substring_regex(s, r"^\d{2}/\d{2}/\d{4}$"), s, None),
                     format="%m/%d/%Y", unit="s", error_is_null=True)
    return pc.coalesce(iso, us).cast(pa.date32())


def _empty_to_null(arr: pa.Array) -> pa.Array:
    s = arr.cast(pa.string())
    return pc.if_else(pc.equal(s, ""), pa.nulls(len(s), pa.string()), s)


def normalise_batch(batch: pa.RecordBatch, mapping: dict[str, str]) -> pa.RecordBatch:
    cols: dict[str, pa.Array] = {}
    present = {mapping[n]: batch.column(i) for i, n in enumerate(batch.schema.names) if n in mapping}
    n = batch.num_rows
    for field in BUSINESS_SCHEMA:
        name = field.name
        if name == "received_month_id":
            continue
        if name not in present:
            cols[name] = pa.nulls(n, field.type)
            continue
        raw = present[name]
        if pa.types.is_date(field.type):
            cols[name] = _to_date(raw)
        elif name == "complaint_id":
            cols[name] = pc.cast(pc.utf8_trim_whitespace(raw.cast(pa.string())), pa.int64())
        elif pa.types.is_boolean(field.type):
            if pa.types.is_boolean(raw.type):
                cols[name] = raw
            else:
                s = pc.utf8_lower(_empty_to_null(raw))
                cols[name] = pc.if_else(pc.is_null(s), pa.nulls(n, pa.bool_()), pc.is_in(s, pa.array(["true", "yes", "1"])))
        else:
            cols[name] = _empty_to_null(raw)
    rd = cols["received_date"]
    month_id = pc.add(pc.multiply(pc.year(rd).cast(pa.int64()), 100), pc.month(rd).cast(pa.int64()))
    cols["received_month_id"] = month_id
    return pa.RecordBatch.from_arrays([cols[f.name] for f in BUSINESS_SCHEMA], schema=BUSINESS_SCHEMA)


def bulk_zip_to_parquet(zip_path: Path, parquet_path: Path, *, block_size: int = 64 << 20) -> int:
    """Stream the CSV inside the zip into a normalised parquet file. Returns row count."""
    rows = 0
    tmp = parquet_path.with_suffix(".part")
    with zipfile.ZipFile(zip_path) as zf:
        member = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        with zf.open(member) as raw:
            reader = pacsv.open_csv(
                raw,
                read_options=pacsv.ReadOptions(block_size=block_size),
                parse_options=pacsv.ParseOptions(newlines_in_values=True),
                convert_options=pacsv.ConvertOptions(
                    column_types={name: pa.string() for name in CSV_TO_COLUMN}, strings_can_be_null=False,
                ),
            )
            with pq.ParquetWriter(tmp, BUSINESS_SCHEMA, compression="zstd", use_dictionary=True) as writer:
                for batch in reader:
                    out = normalise_batch(batch, CSV_TO_COLUMN)
                    writer.write_batch(out)
                    rows += out.num_rows
    tmp.replace(parquet_path)
    return rows


# ------------------------------------------------------------------ API path (optional)
API_FIELDS = [k for k in API_TO_COLUMN if k != "has_narrative"]  # never complaint_what_happened


def fetch_api_window(settings: Settings, start: date, end: date, *, page_size: int = 1000) -> pa.Table:
    """Pull complaints received in [start, end] from the JSON API using search_after pagination.

    Only business fields are requested; the narrative is never part of the request.
    Raises SourceError with a plain message when the API is geo-fenced (HTTP 403).
    """
    s = session()
    params = {
        "date_received_min": start.isoformat(), "date_received_max": end.isoformat(), "size": page_size,
        "sort": "created_date_desc", "no_aggs": "true", "format": "json", "field": "all",
    }
    rows: list[dict] = []
    search_after = None
    while True:
        p = dict(params)
        if search_after:
            p["search_after"] = search_after
        r = get_with_retry(s, settings.cfpb_api_url, params=p, timeout=settings.request_timeout_s,
                           max_attempts=settings.max_attempts)
        body = r.json()
        hits = body.get("hits", {}).get("hits", [])
        if not hits:
            break
        for h in hits:
            src = h["_source"]
            rows.append({
                API_TO_COLUMN[k]: src.get(k) for k in API_TO_COLUMN if k != "has_narrative"
            } | {"has_narrative": bool(src.get("complaint_what_happened"))})
        last = hits[-1]
        sort_vals = last.get("sort")
        if not sort_vals or len(hits) < page_size:
            break
        search_after = "_".join(str(v) for v in sort_vals)
    if not rows:
        return BUSINESS_SCHEMA.empty_table()
    names = list(API_TO_COLUMN.values())
    arrays = {}
    for name in names:
        if name == "has_narrative":
            arrays[name] = pa.array([bool(r.get(name)) for r in rows], pa.bool_())
        else:
            vals = []
            for r in rows:
                v = r.get(name)
                if isinstance(v, str) and name in ("received_date", "date_sent_to_company"):
                    v = v[:10]
                vals.append(None if v is None else str(v))
            arrays[name] = pa.array(vals, pa.string())
    batch = pa.RecordBatch.from_pydict(arrays)
    return pa.Table.from_batches([normalise_batch(batch, {k: k for k in arrays})])


def write_table(table: pa.Table, path: Path) -> None:
    pq.write_table(table, path, compression="zstd")
