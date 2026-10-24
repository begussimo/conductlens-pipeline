"""Raw table schemas, in one place, as pyarrow schemas plus per-warehouse DDL.

The pyarrow schema is the truth: parquet files are written with it, BigQuery load jobs
and Databricks MERGE both consume those files, so a column added here lands everywhere.
"""
from __future__ import annotations

import pyarrow as pa

AUDIT_FIELDS = [
    pa.field("row_hash", pa.string(), nullable=False),
    pa.field("_source", pa.string(), nullable=False),
    pa.field("_source_as_of", pa.date32(), nullable=True),
    pa.field("_loaded_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("_run_id", pa.string(), nullable=False),
    pa.field("_is_tombstone", pa.bool_(), nullable=False),
]

CFPB_BUSINESS_FIELDS = [
    pa.field("complaint_id", pa.int64(), nullable=False),
    pa.field("received_date", pa.date32(), nullable=True),
    pa.field("received_month_id", pa.int64(), nullable=True),
    pa.field("product", pa.string()),
    pa.field("sub_product", pa.string()),
    pa.field("issue", pa.string()),
    pa.field("sub_issue", pa.string()),
    pa.field("company_public_response", pa.string()),
    pa.field("company", pa.string()),
    pa.field("state", pa.string()),
    pa.field("zip_code", pa.string()),
    pa.field("tags", pa.string()),
    pa.field("submitted_via", pa.string()),
    pa.field("date_sent_to_company", pa.date32()),
    pa.field("company_response", pa.string()),
    pa.field("timely_response", pa.string()),
    pa.field("consumer_disputed", pa.string()),
    pa.field("consumer_consent_provided", pa.string()),
    pa.field("has_narrative", pa.bool_()),
]
# Hash order per ARCHITECTURE §3.1 (business key excluded).
CFPB_HASH_COLUMNS = [
    "received_date", "product", "sub_product", "issue", "sub_issue", "company_public_response", "company",
    "state", "zip_code", "tags", "submitted_via", "date_sent_to_company", "company_response", "timely_response",
    "consumer_disputed", "consumer_consent_provided", "has_narrative",
]
CFPB_SCHEMA = pa.schema(CFPB_BUSINESS_FIELDS + AUDIT_FIELDS)

# BigQuery-only compact grain (ARCHITECTURE §3.1b). Audit columns without _run_id to save bytes.
CFPB_DAILY_CODE_DIMENSIONS = [
    "company", "product", "sub_product", "issue", "state", "company_response", "timely_response", "consumer_disputed",
]
CFPB_DAILY_BUSINESS_FIELDS = (
    [pa.field("received_date", pa.date32(), nullable=False), pa.field("received_month_id", pa.int64(), nullable=False)]
    + [pa.field(f"{d}_code", pa.int64(), nullable=False) for d in CFPB_DAILY_CODE_DIMENSIONS]
    + [pa.field("n_complaints", pa.int64(), nullable=False)]
)
CFPB_DAILY_SCHEMA = pa.schema(CFPB_DAILY_BUSINESS_FIELDS + [f for f in AUDIT_FIELDS if f.name != "_run_id"])
CFPB_DAILY_KEYS = ["received_date"] + [f"{d}_code" for d in CFPB_DAILY_CODE_DIMENSIONS]
CFPB_CODES_SCHEMA = pa.schema(
    [
        pa.field("dimension", pa.string(), nullable=False),
        pa.field("code", pa.int64(), nullable=False),
        pa.field("value", pa.string(), nullable=False),
        pa.field("_loaded_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("_run_id", pa.string(), nullable=False),
    ]
)

FDIC_BUSINESS_FIELDS = [
    pa.field("cert", pa.int64(), nullable=False),
    pa.field("name", pa.string()),
    pa.field("city", pa.string()),
    pa.field("stalp", pa.string()),
    pa.field("bkclass", pa.string()),
    pa.field("active", pa.bool_()),
    pa.field("fed_rssd", pa.string()),
    pa.field("rssdhcr", pa.string()),
    pa.field("namehcr", pa.string()),
    pa.field("asset_thousands", pa.int64()),
    pa.field("dep_thousands", pa.int64()),
    pa.field("offices", pa.int64()),
    pa.field("repdte", pa.date32()),
    pa.field("dateupdt", pa.date32()),
    pa.field("estymd", pa.date32()),
    pa.field("endefymd", pa.date32()),
    pa.field("webaddr", pa.string()),
]
FDIC_HASH_COLUMNS = [f.name for f in FDIC_BUSINESS_FIELDS if f.name != "cert"]
FDIC_SCHEMA = pa.schema(FDIC_BUSINESS_FIELDS + AUDIT_FIELDS)
FDIC_API_FIELDS = [
    "CERT", "NAME", "CITY", "STALP", "BKCLASS", "ACTIVE", "FED_RSSD", "RSSDHCR", "NAMEHCR", "ASSET", "DEP",
    "OFFICES", "REPDTE", "DATEUPDT", "ESTYMD", "ENDEFYMD", "WEBADDR",
]

FRED_BUSINESS_FIELDS = [
    pa.field("series_id", pa.string(), nullable=False),
    pa.field("observation_date", pa.date32(), nullable=False),
    pa.field("value", pa.float64()),
]
FRED_HASH_COLUMNS = ["series_id", "observation_date", "value"]
FRED_SCHEMA = pa.schema(FRED_BUSINESS_FIELDS + AUDIT_FIELDS)

LOAD_RUNS_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string()),
        pa.field("source", pa.string()),
        pa.field("sink", pa.string()),
        pa.field("mode", pa.string()),
        pa.field("window_start", pa.date32()),
        pa.field("window_end", pa.date32()),
        pa.field("rows_read", pa.int64()),
        pa.field("rows_written", pa.int64()),
        pa.field("status", pa.string()),
        pa.field("error", pa.string()),
        pa.field("started_at", pa.timestamp("us", tz="UTC")),
        pa.field("finished_at", pa.timestamp("us", tz="UTC")),
    ]
)
LOAD_STATE_SCHEMA = pa.schema(
    [
        pa.field("source", pa.string()),
        pa.field("sink", pa.string()),
        pa.field("watermark_date", pa.date32()),
        pa.field("source_as_of", pa.date32()),
        pa.field("last_run_id", pa.string()),
        pa.field("updated_at", pa.timestamp("us", tz="UTC")),
    ]
)
PARITY_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string()),
        pa.field("table_name", pa.string()),
        pa.field("metric", pa.string()),
        pa.field("bigquery_value", pa.float64()),
        pa.field("databricks_value", pa.float64()),
        pa.field("rel_diff", pa.float64()),
        pa.field("status", pa.string()),
        pa.field("checked_at", pa.timestamp("us", tz="UTC")),
    ]
)


class TableSpec:
    def __init__(self, name: str, schema: pa.Schema, key_columns: list[str], hash_columns: list[str],
                 window_column: str | None, bq_partition: dict | None, bq_cluster: list[str], dbx_cluster: list[str],
                 hash_length: int = 32, sinks: tuple[str, ...] = ("bigquery", "databricks", "local")):
        self.name = name
        self.schema = schema
        self.key_columns = key_columns
        self.hash_columns = hash_columns
        self.window_column = window_column
        self.bq_partition = bq_partition
        self.bq_cluster = bq_cluster
        self.dbx_cluster = dbx_cluster
        self.hash_length = hash_length
        self.sinks = sinks  # which sinks create/hold this table


TABLES: dict[str, TableSpec] = {
    "cfpb_complaints": TableSpec(
        "cfpb_complaints", CFPB_SCHEMA, ["complaint_id"], CFPB_HASH_COLUMNS, "received_date",
        {"field": "received_month_id", "start": 201101, "end": 203612, "interval": 1},
        ["company", "product"], ["received_date", "company"],
    ),
    "cfpb_complaints_daily": TableSpec(
        "cfpb_complaints_daily", CFPB_DAILY_SCHEMA, CFPB_DAILY_KEYS, ["n_complaints"], "received_date",
        {"field": "received_month_id", "start": 201101, "end": 203612, "interval": 1},
        ["company_code", "product_code"], ["received_date", "company_code"], hash_length=16, sinks=("bigquery", "local"),
    ),
    "fdic_institutions": TableSpec(
        "fdic_institutions", FDIC_SCHEMA, ["cert"], FDIC_HASH_COLUMNS, None, None, ["cert"], ["cert"],
    ),
    "fred_observations": TableSpec(
        "fred_observations", FRED_SCHEMA, ["series_id", "observation_date"], FRED_HASH_COLUMNS, "observation_date",
        None, ["series_id"], ["series_id", "observation_date"],
    ),
}
META_TABLES: dict[str, pa.Schema] = {
    "cfpb_codes": CFPB_CODES_SCHEMA,
    "_load_runs": LOAD_RUNS_SCHEMA,
    "_load_state": LOAD_STATE_SCHEMA,
    "_parity_checks": PARITY_SCHEMA,
}

_PA_TO_BQ = {
    pa.int64(): "INT64", pa.string(): "STRING", pa.date32(): "DATE", pa.bool_(): "BOOL", pa.float64(): "FLOAT64",
    pa.timestamp("us", tz="UTC"): "TIMESTAMP",
}
_PA_TO_DBX = {
    pa.int64(): "BIGINT", pa.string(): "STRING", pa.date32(): "DATE", pa.bool_(): "BOOLEAN", pa.float64(): "DOUBLE",
    pa.timestamp("us", tz="UTC"): "TIMESTAMP",
}


def bq_schema(schema: pa.Schema):
    from google.cloud import bigquery

    return [
        bigquery.SchemaField(f.name, _PA_TO_BQ[f.type], mode="NULLABLE" if f.nullable else "REQUIRED") for f in schema
    ]


def dbx_columns_ddl(schema: pa.Schema) -> str:
    return ", ".join(f"`{f.name}` {_PA_TO_DBX[f.type]}{'' if f.nullable else ' NOT NULL'}" for f in schema)


def duckdb_columns_ddl(schema: pa.Schema) -> str:
    m = {pa.int64(): "BIGINT", pa.string(): "VARCHAR", pa.date32(): "DATE", pa.bool_(): "BOOLEAN",
         pa.float64(): "DOUBLE", pa.timestamp("us", tz="UTC"): "TIMESTAMPTZ"}
    return ", ".join(f'"{f.name}" {m[f.type]}' for f in schema)
