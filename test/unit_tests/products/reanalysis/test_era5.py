"""
Tests for the pansat.products.reanalysis.era5 module.
"""

from datetime import datetime
import os
import pytest
import pansat.products.reanalysis.era5 as era5



PRODUCTS = [
    era5.ERA5Product("hourly", "surface", ["2m_temperature"]),
    era5.ERA5Product("monthly", "land", ["asn"], domain = [25,50,70,120]),
]



TEST_NAMES = {
    "reanalysis-era5-single-levels": "reanalysis-era5-single-levels_2016100115_2m_temperature.nc",
    "reanalysis-era5-land-monthly-means": "reanalysis-era5-land-monthly-means_201610_asn.nc"}


TEST_TIMES = {
    "reanalysis-era5-single-levels": datetime(2016,10,1,15),
    "reanalysis-era5-land-monthly-means": datetime(2016, 10, 1, 0)
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
    tmp_dir = tmpdir_factory.mktemp("data")
    return tmp_dir


def test_download_hourly(tmpdir):
    product = PRODUCTS[0]
    t_0 = datetime(2016, 10, 1, 15)
    t_1 = datetime(2016, 10, 1, 17)
    product.download(t_0, t_1, str(tmpdir))


def test_download_monthly(tmpdir):
    product = PRODUCTS[1]
    t_0 = datetime(2016, 10, 1, 0)
    t_1 = datetime(2016, 11, 1, 0)
    product.download(t_0, t_1, str(tmpdir))


def test_open(tmpdir):
    product = PRODUCTS[0]
    fn = str(tmpdir) + ("/") + TEST_NAMES[product.name]
    product.open(fn)
