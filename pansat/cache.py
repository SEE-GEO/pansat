"""
pansat.cache
============

Provides functions to cache web requests and pansat files.
"""
import logging
import os

import requests
from requests_cache import CachedSession

from pansat.download.accounts import _APP_DIR


LOGGER = logging.getLogger(__name__)


REQUESTS_CACHE = _APP_DIR / "requests_cache"
DISABLE = False


def is_active() -> bool:
    """
    Determine whether cache should be used for downloads.
    """
    if "PANSAT_DISABLE_CACHE" in os.environ:
        LOGGER.info("Disabling cache.")
        return False
    else:
        return not DISABLE


def get_session():
    """
    Get requests session that caches request in pansat user directory.
    """
    if not is_active():
        return requests.Session()

    session = CachedSession(REQUESTS_CACHE)
    return session
