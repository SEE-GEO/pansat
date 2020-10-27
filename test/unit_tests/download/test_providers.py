"""
Tests for provider classes and download functions.

"""

import pytest
import pansat.download.accounts as accs
import pansat.download.providers as provs
import random
import datetime
import os


@pytest.fixture(autouse=True)
def setup_cds_identity(monkeypatch):
    url, key = accs.get_identity("Copernicus")
    os.environ["CDSAPI_URL"] = url
    os.environ["CDSAPI_KEY"] = key


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ
