"""
Test for NOAA NCEI provider.
"""
from datetime import datetime
from random import randint

import pytest

from pansat import TimeRange
from pansat.products.satellite.ncei import gridsat_goes, gridsat_b1, ssmi_csu, isccp_hgm


def test_find_provider():
    """
    Ensure that a provider for GridSat GOES is found.
    """
    provider = gridsat_goes.find_provider()
    assert provider is not None


def test_noaa_ncei_provider_monthly():
    """
    Test NOAA NCEI provider for files listed by month.
    """
    time_range = TimeRange("2000-01-01T00:00:01", "2000-01-01T23:29:59")
    files = gridsat_goes.find_files(time_range)
    # We expect 48 product because two GOES satellites are available.
    assert len(files) == 48


def test_noaa_ncei_provider_year():
    """
    Test NOAA NCEI provider for files listed by year.
    """
    time_range = TimeRange("2000-01-01T00:00:01", "2000-01-01T22:29:59")
    files = gridsat_b1.find_files(time_range)
    # We expect 8 products.
    assert len(files) == 8


def test_noaa_ncei_provider_all():
    """
    Test NOAA NCEI provider for files listed without subfolders..
    """
    time_range = TimeRange("2000-01-01T00:00:01", "2000-01-01T22:29:59")
    files = isccp_hgm.find_files(time_range)
    assert len(files) == 1


@pytest.mark.slow
def test_noaa_ncei_download(tmp_path):
    """
    Test NOAA NCEI provider for files listed by year.
    """
    products = [gridsat_b1, ssmi_csu]

    date = datetime(2000, 1, 1, randint(0, 23))
    for product in products:
        files = product.find_files(TimeRange(date, date))
        rec = files[0].get()
        assert rec.local_path.exists()
