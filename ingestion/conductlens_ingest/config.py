"""Runtime settings. Everything is an environment variable (see .env.example).

Secrets (service-account path, Databricks token) are read by the client libraries
themselves; this module never logs or returns their values.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()  # a .env in the CWD, if present; real env vars win


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    data_dir: Path = field(default_factory=lambda: Path(_env("CL_DATA_DIR", "./data")).expanduser())
    sinks: tuple[str, ...] = field(
        default_factory=lambda: tuple(s.strip() for s in _env("CL_SINKS", "bigquery,databricks").split(",") if s.strip())
    )
    our_company: str = field(default_factory=lambda: _env("CL_OUR_COMPANY", "ALLY FINANCIAL INC."))

    # CFPB
    cfpb_bulk_url: str = field(
        default_factory=lambda: _env("CL_CFPB_BULK_URL", "https://files.consumerfinance.gov/ccdb/complaints.csv.zip")
    )
    cfpb_api_url: str = field(
        default_factory=lambda: _env(
            "CL_CFPB_API_URL", "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
        )
    )
    cfpb_source: str = field(default_factory=lambda: _env("CL_CFPB_SOURCE", "bulk_csv"))  # bulk_csv | api
    cfpb_overlap_days: int = field(default_factory=lambda: _env_int("CL_CFPB_OVERLAP_DAYS", 60))
    history_start: str = field(default_factory=lambda: _env("CL_HISTORY_START", "2011-12-01"))

    # FRED
    fred_series: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            s.strip() for s in _env("CL_FRED_SERIES", "DGS10,DFF,DPRIME,MORTGAGE30US,TERMCBCCALLNS").split(",") if s.strip()
        )
    )
    fred_overlap_days: int = field(default_factory=lambda: _env_int("CL_FRED_OVERLAP_DAYS", 90))

    # FDIC
    fdic_api_url: str = field(default_factory=lambda: _env("CL_FDIC_API_URL", "https://api.fdic.gov/banks"))

    # BigQuery
    bq_project: str | None = field(default_factory=lambda: _env("CL_BQ_PROJECT"))
    bq_location: str = field(default_factory=lambda: _env("CL_BQ_LOCATION", "US"))
    bq_prefix: str = field(default_factory=lambda: _env("CL_BQ_DATASET_PREFIX", "conductlens"))
    bq_cfpb_grain: str = field(default_factory=lambda: _env("CL_BQ_CFPB_GRAIN", "daily_cells"))  # daily_cells | complaints
    bq_rotate_days_before_expiry: int = field(default_factory=lambda: _env_int("CL_BQ_ROTATE_DAYS_BEFORE_EXPIRY", 5))

    # Databricks
    dbx_http_path: str | None = field(default_factory=lambda: _env("DATABRICKS_HTTP_PATH"))
    dbx_catalog: str = field(default_factory=lambda: _env("CL_DBX_CATALOG", "workspace"))
    dbx_prefix: str = field(default_factory=lambda: _env("CL_DBX_SCHEMA_PREFIX", "conductlens"))
    dbx_volume: str = field(default_factory=lambda: _env("CL_DBX_VOLUME", "landing"))

    # Local DuckDB sink (dev / tests)
    local_db_path: Path = field(default_factory=lambda: Path(_env("CL_LOCAL_DB", "./data/local_sink.duckdb")))

    request_timeout_s: int = field(default_factory=lambda: _env_int("CL_REQUEST_TIMEOUT_S", 300))
    max_attempts: int = field(default_factory=lambda: _env_int("CL_MAX_ATTEMPTS", 5))
    keep_landed_files: int = field(default_factory=lambda: _env_int("CL_KEEP_LANDED_FILES", 3))

    @property
    def bq_raw_dataset(self) -> str:
        return f"{self.bq_prefix}_raw"

    @property
    def dbx_raw_schema(self) -> str:
        return f"{self.dbx_prefix}_raw"

    @property
    def mart_schema_name(self) -> str:
        # identical suffix on both warehouses; prefix may differ per warehouse
        return "marts"
