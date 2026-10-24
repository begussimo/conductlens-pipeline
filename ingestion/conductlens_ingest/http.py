"""One requests session with retries; no secrets ever pass through here."""
from __future__ import annotations

import logging
import time

import requests

log = logging.getLogger(__name__)
USER_AGENT = "conductlens-pipeline/0.1"  # FRED drops connections for UAs containing parentheses


class SourceError(RuntimeError):
    """A source answered with something we cannot use. Message is safe to log."""


def session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def get_with_retry(s: requests.Session, url: str, *, params=None, headers=None, timeout: int, max_attempts: int,
                   stream: bool = False) -> requests.Response:
    delay = 5.0
    last: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = s.get(url, params=params, headers=headers, timeout=timeout, stream=stream)
            if r.status_code in (429, 500, 502, 503, 504):
                raise SourceError(f"HTTP {r.status_code} from {url.split('?')[0]}")
            if r.status_code == 403:
                # consumerfinance.gov geo-fences its JSON API (Akamai 'Access Denied'); the bulk
                # file host does not. Fail fast with a message that says what to do.
                raise SourceError(f"HTTP 403 from {url.split('?')[0]}: blocked for this network (geo-fence?)")
            if r.status_code != 304:
                r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout, SourceError) as exc:
            last = exc
            if isinstance(exc, SourceError) and "403" in str(exc):
                raise
            log.warning("attempt %d/%d failed: %s", attempt, max_attempts, exc)
            if attempt < max_attempts:
                time.sleep(delay)
                delay = min(delay * 2, 120)
    raise SourceError(f"giving up after {max_attempts} attempts: {last}")
