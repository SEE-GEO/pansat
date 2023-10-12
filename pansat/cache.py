"""
pansat.cache
============

Provides functions to cache web requests and pansat files.
"""
from requests_cache import CachedSession

from pansat.download.accounts import _APP_DIR

REQUESTS_CACHE = _APP_DIR / "requests_cache"


def get_session():
    """
    Get requests session that caches request in pansat user directory.
    """
    session = CachedSession(REQUESTS_CACHE)
    return session
