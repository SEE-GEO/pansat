"""
Tests for the Opera product.
"""
from datetime import datetime
import os
import pytest
import pansat.products.ground_based.opera as opera

import numpy as np

PRODUCTS = [opera.rainfall_rate, opera.maximum_reflectivity]
TEST_NAMES = {
    str(opera.rainfall_rate): ("OPERA_RAINFALL_RATE_2020_275_10_15.hdf"),
    str(opera.maximum_reflectivity): (
        "OPERA_MAXIMUM_REFLECTIVITY" "_2020_275_10_15.hdf"
    ),
}
TEST_TIMES = {
    str(opera.rainfall_rate): datetime(2020, 10, 1, 10, 15),
    str(opera.maximum_reflectivity): datetime(2020, 10, 1, 10, 15),
}

HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.parametrize("product", PRODUCTS)
def test_matches(product):
    """
    Assert that matches method returns true on the filename.
    """
    filename = TEST_NAMES[str(product)]
    assert product.matches(filename)


@pytest.mark.parametrize("product", PRODUCTS)
def test_filename_to_date(product):
    """
    Assert that matches method returns true on the filename.
    """
    filename = TEST_NAMES[str(product)]
    reference_time = TEST_TIMES[str(product)]
    time = product.filename_to_date(filename)
    assert time == reference_time


def test_grids():
    lats = opera.get_latitude_grid()
    lons = opera.get_longitude_grid()

    assert np.isclose(lats.min(), 31.7575)
    assert np.isclose(lats.max(), 73.918)
    assert np.isclose(lons.min(), -39.5024)
    assert np.isclose(lons.max(), 57.7779)


@pytest.mark.skip(reason="Product outdated.")
@pytest.mark.usefixtures("test_identities")
def test_download():
    """
    Download CloudSat L1B file.
    """
    product = opera.maximum_reflectivity
    t_0 = datetime(2018, 6, 1, 10)
    t_1 = datetime(2018, 6, 1, 10, 15)
    product.download(t_0, t_1)
