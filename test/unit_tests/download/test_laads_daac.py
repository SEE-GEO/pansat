"""
Test for the LAADS DAAC provider.
"""

import os

import pytest

from pansat.download.providers import LAADSDAACProvider
from pansat.products.satellite.modis import modis_terra_1km


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


def test_get_files_by_day():
    """Assert that number of files per day matches 12 * 24 = 288"""
    assert str(modis_terra_1km) in LAADSDAACProvider.get_available_products()
    provider = LAADSDAACProvider(modis_terra_1km)
    files = provider.get_files_by_day(2020, 1)
    assert len(files) == 24 * 12


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_download(tmp_path):
    """Assert that number of files per day matches 12 * 24 = 288"""
    provider = LAADSDAACProvider(modis_terra_1km)
    files = provider.get_files_by_day(2020, 1)
    provider.download_file(files[0], tmp_path / "test.hdf")
