from datetime import date

import pyarrow.parquet as pq
from conductlens_ingest.schema import CFPB_SCHEMA
from conductlens_ingest.sources.cfpb import BUSINESS_SCHEMA, bulk_zip_to_parquet


def test_bulk_zip_to_parquet_types_and_values(base_zip, tmp_path):
    out = tmp_path / "c.parquet"
    n = bulk_zip_to_parquet(base_zip, out)
    assert n == 6  # duplicates are kept here; dedupe happens in the runner
    t = pq.read_table(out)
    assert t.schema.equals(BUSINESS_SCHEMA)
    assert set(t.schema.names) <= set(CFPB_SCHEMA.names)
    rows = {r["complaint_id"]: r for r in t.to_pylist()}
    assert rows[1]["received_date"] == date(2026, 10, 1)
    assert rows[1]["received_month_id"] == 202610
    assert rows[4]["received_month_id"] == 202506
    assert rows[1]["timely_response"] == "Yes"
    assert rows[3]["timely_response"] is None          # empty string -> null
    assert rows[5]["date_sent_to_company"] is None      # empty date -> null
    assert rows[5]["sub_product"] == "Virtual currency"
    assert rows[1]["consumer_disputed"] is None         # API-only column, absent from the bulk file
    assert rows[1]["has_narrative"] is None
