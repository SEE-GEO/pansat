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
    read_config = lambda x: {"url": url, "key": key}
    monkeypatch.setattr("cdsapi.api.read_config", read_config)


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_copernicus_provider(tmpdir):
    """
    This test creates an instance for CopernicusProvider class and downloads global air temperatures for a randomly selected data product among available ERA5 reanalysis datasets.
    """
    product = random.choice(provs.copernicus_products)
    variable = "2m_temperature"
    if "pressure" in product:
        variable = "temperature"
    era = provs.CopernicusProvider(product, variable)

    start = datetime.datetime(2000, 1, 1, 10)
    end = datetime.datetime(2000, 1, 1, 11)
    dest = tmpdir

    era.download(start, end, dest)

    assert dest.exists()
