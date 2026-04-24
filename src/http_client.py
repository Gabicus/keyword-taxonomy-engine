"""HTTP client with caching, retries, and per-source configuration."""

import requests
import requests_cache
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

from .config import RAW_DIR, USER_AGENT


def get_session(cache_name: str = "keyword_cache", use_cache: bool = True) -> requests.Session:
    """Create a configured requests session with caching and retries."""
    cache_dir = RAW_DIR / "http_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    if use_cache:
        session = requests_cache.CachedSession(
            str(cache_dir / cache_name),
            expire_after=86400 * 7,  # 7 day cache
            allowable_methods=["GET"],
        )
    else:
        session = requests.Session()

    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/csv,application/json,application/rdf+xml,*/*",
    })

    return session
