"""FDIC BankFind institutions: all charters (active and inactive), full pull every run."""
from __future__ import annotations

import logging
from datetime import date, datetime

import pyarrow as pa

from ..config import Settings
from ..http import get_with_retry, session
from ..schema import FDIC_API_FIELDS, FDIC_BUSINESS_FIELDS

log = logging.getLogger(__name__)
BUSINESS_SCHEMA = pa.schema(FDIC_BUSINESS_FIELDS)
PAGE = 10_000  # API maximum


def _date(v) -> date | None:
    if v in (None, "", "0"):
        return None
    s = str(v)[:10]
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _int(v) -> int | None:
    if v in (None, ""):
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _str(v) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def record_to_row(d: dict) -> dict:
    return {
        "cert": _int(d.get("CERT")),
        "name": _str(d.get("NAME")),
        "city": _str(d.get("CITY")),
        "stalp": _str(d.get("STALP")),
        "bkclass": _str(d.get("BKCLASS")),
        "active": None if d.get("ACTIVE") in (None, "") else str(d.get("ACTIVE")) in ("1", "true", "True"),
        "fed_rssd": _str(d.get("FED_RSSD")),
        "rssdhcr": _str(d.get("RSSDHCR")) if _str(d.get("RSSDHCR")) not in ("0",) else None,
        "namehcr": _str(d.get("NAMEHCR")),
        "asset_thousands": _int(d.get("ASSET")),
        "dep_thousands": _int(d.get("DEP")),
        "offices": _int(d.get("OFFICES")),
        "repdte": _date(d.get("REPDTE")),
        "dateupdt": _date(d.get("DATEUPDT")),
        "estymd": _date(d.get("ESTYMD")),
        "endefymd": _date(d.get("ENDEFYMD")),
        "webaddr": _str(d.get("WEBADDR")),
    }


def fetch_institutions(settings: Settings, *, active_only: bool = False) -> pa.Table:
    s = session()
    rows: list[dict] = []
    offset = 0
    while True:
        params = {"fields": ",".join(FDIC_API_FIELDS), "limit": PAGE, "offset": offset, "sort_by": "CERT",
                  "sort_order": "ASC", "format": "json"}
        if active_only:
            params["filters"] = "ACTIVE:1"
        r = get_with_retry(s, f"{settings.fdic_api_url}/institutions", params=params,
                           timeout=settings.request_timeout_s, max_attempts=settings.max_attempts)
        body = r.json()
        data = body.get("data", [])
        rows.extend(record_to_row(item.get("data", item)) for item in data)
        total = int(body.get("meta", {}).get("total") or body.get("totals", {}).get("count") or 0)
        offset += len(data)
        if not data or offset >= total:
            break
    rows = [r for r in rows if r["cert"] is not None]
    log.info("fdic: %d institutions", len(rows))
    return pa.Table.from_pylist(rows, schema=BUSINESS_SCHEMA)
