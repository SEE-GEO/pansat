"""
Test for NOAA NCEI provider.
"""
from datetime import datetime
from random import randint

import pytest

from pansat import TimeRange
from pansat.products.satellite.ncei import (
    gridsat_goes,
    gridsat_b1,
    ssmi_csu,
    isccp_hgm,
    isccp_hxg
)
from pansat.products.dem.globe import globe
from pansat.geometry import LonLatRect


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
    Test NOAA NCEI provider for files listed without subfolders.
    """
    time_range = TimeRange("2000-01-01T00:00:01", "2000-01-01T22:29:59")
    files = isccp_hgm.find_files(time_range)
    assert len(files) == 1


def test_find_isccp_hxg_files():
    """
    Ensure that providers finds ISCCP HXG files.
    """
    time_range = TimeRange("2001-01-01", "2001-01-02")
    recs = isccp_hxg.find_files(time_range)
    assert len(recs) == 9


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


def test_noaa_globe_provider():
    """
    Test that provider for NOAA GLOBE DEM data is available.
    """
    recs = globe.find_files(TimeRange("2020-01-01"))
    assert len(recs) == 16

    roi = LonLatRect(10, 10, 20, 20)
    recs = globe.find_files(TimeRange("2020-01-01"), roi=roi)
    assert len(recs) == 1


@pytest.mark.slow
def test_noaa_globe_provider_download(tmp_path):
    """
    Test that provider for NOAA GLOBE DEM data is available.
    """
    roi = LonLatRect(10, 10, 20, 20)
    recs = globe.find_files(TimeRange("2020-01-01"), roi=roi)
    recs[0].get(destination=tmp_path)
    assert (tmp_path / "noaa" / "globe").exists()
