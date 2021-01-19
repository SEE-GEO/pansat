"""
Tests for the pansat.products.reanalysis.era5 module.
"""

from datetime import datetime
import random
import os
import sys
import pytest
import pansat.products.reanalysis.era5 as era5


PRODUCTS = [
    era5.ERA5Hourly("surface", ["2m_temperature"]),
    era5.ERA5Monthly("land", ["asn"], domain=[25, 50, 70, 120]),
]

TEST_NAMES = {
    "reanalysis-era5-single-levels": "reanalysis-era5-single-levels_2016100115_2m_temperature.nc",
    "reanalysis-era5-land-monthly-means": "reanalysis-era5-land-monthly-means_201610_asn25-50-70-120.nc",
}


TEST_TIMES = {
    "reanalysis-era5-single-levels": datetime(2016, 10, 1, 15),
    "reanalysis-era5-land-monthly-means": datetime(2016, 10, 1, 0),
}


@pytest.mark.parametrize("product", PRODUCTS)
def test_filename_to_date(product):
    """
    Assert that time is correctly extracted from filename.
    """
    filename = TEST_NAMES[product.name]
    time = product.filename_to_date(filename)
    assert time == TEST_TIMES[product.name]


@pytest.mark.parametrize("product", PRODUCTS)
def test_matches(product):
    """
    Assert that matches method returns true on the filename.
    """
    filename = TEST_NAMES[product.name]
    assert product.matches(filename)


@pytest.fixture(scope="session")
def tmpdir(tmpdir_factory):
    """
    Creates temporary directory for test session.

    """
    tmp_dir = tmpdir_factory.mktemp(f"data{random.randint(1,300)}")
    return tmp_dir


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.skipif(sys.platform.startswith("win"), reason="Does not work on Windows")
@pytest.mark.usefixtures("test_identities")
def test_download(tmpdir):
    """
    Test downloading and opening of a monthly and an hourly ERA5 file.

    """
    # hourly
    product = PRODUCTS[0]
    t_0 = datetime(2016, 10, 1, 15)
    t_1 = datetime(2016, 10, 1, 17)
    product.download(t_0, t_1, str(tmpdir))
    # monthly
    product = PRODUCTS[1]
    t_0 = datetime(2016, 10, 1, 0)
    t_1 = datetime(2016, 11, 1, 0)
    product.download(t_0, t_1, str(tmpdir))
    # open file
    filename = tmpdir / TEST_NAMES[product.name]
    data = product.open(str(filename))
    data.close()
