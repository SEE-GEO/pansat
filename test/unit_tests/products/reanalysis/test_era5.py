"""
Tests for the pansat.products.reanalysis.era5 module.
"""

from datetime import datetime, timedelta
import random
import os
import sys
import pytest
import pansat.products.reanalysis.era5 as era5
from pansat.geometry import LonLatRect


PRODUCTS = [
    era5.ERA5Hourly(levels="surface", variables=["2m_temperature"]),
    era5.ERA5Monthly(levels="land",  variables=["asn"], domain=(70, 25, 120, 50)),
]

TEST_NAMES = {
    "reanalysis.era5_surface_hourly_[2m_temperature]": "era5_surface_hourly_2016101500_2016101600_[2m_temperature].nc",
    "reanalysis.era5_land_monthly_[asn]_[70,25,120,50]": "era5_land_monthly_201610_201611_[asn]_[70,25,120,50].nc",
}

TEST_TIMES = {
    "reanalysis.era5_surface_hourly_[2m_temperature]": datetime(2016, 10, 15),
    "reanalysis.era5_land_monthly_[asn]_[70,25,120,50]": datetime(2016, 10, 1, 0),
}

TEST_DOMAINS = {
    "reanalysis.era5_surface_hourly_[2m_temperature]": LonLatRect(-180, -90, 180, 90),
    "reanalysis.era5_land_monthly_[asn]_[70,25,120,50]": LonLatRect(70, 25, 120, 50),
}


@pytest.mark.parametrize("product", PRODUCTS)
def test_get_temporal_coverage(product):
    """
    Assert that time is correctly extracted from filename.
    """
    filename = TEST_NAMES[product.name]
    time_range = product.get_temporal_coverage(filename)
    assert time_range.start == TEST_TIMES[product.name]


@pytest.mark.parametrize("product", PRODUCTS)
def test_get_spatial_coverage(product):
    """
    Assert that time is correctly extracted from filename.
    """
    filename = TEST_NAMES[product.name]
    domain = product.get_spatial_coverage(filename)
    bbox_era5 = domain.bounding_box_corners
    bbox_ref = TEST_DOMAINS[product.name].bounding_box_corners
    assert bbox_era5 == bbox_ref



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


@pytest.mark.parametrize("product", PRODUCTS)
def test_get_product(product):
    """
    Ensure that 'get_product' return the expected product object.
    """
    prod = era5.get_product(product.name)
    assert prod.name == product.name
