"""
Tests for the Opera product.
"""
from datetime import datetime
import os

import pytest
from conftest import NEEDS_PANSAT_PASSWORD
import numpy as np

from pansat import TimeRange
import pansat.products.ground_based.opera as opera


PRODUCTS = [opera.precip_rate, opera.reflectivity]
TEST_NAMES = {
    opera.precip_rate.name: ("20180101_RAINFALL_RATE.tar"),
    opera.reflectivity.name: ("20180101_REFLECTIVITY.tar"),
}
TEST_TIMES = {
    opera.precip_rate.name: datetime(2018, 1, 1, 0, 0),
    opera.reflectivity.name: datetime(2018, 1, 1, 0, 0),
}

HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.parametrize("product", PRODUCTS)
def test_matches(product):
    """
    Assert that matches method returns true on the filename.
    """
    filename = TEST_NAMES[product.name]
    assert product.matches(filename)


@pytest.mark.parametrize("product", PRODUCTS)
def test_filename_to_date(product):
    """
    Assert that matches method returns true on the filename.
    """
    filename = TEST_NAMES[product.name]
    reference_time = TEST_TIMES[product.name]
    time = product.filename_to_date(filename)
    assert time == reference_time


@NEEDS_PANSAT_PASSWORD
@pytest.mark.usefixtures("test_identities")
@pytest.mark.slow
def test_open_file(tmp_path):
    """
    Assert that matches method returns true on the filename.
    """
    time_range = TimeRange("2020-01-01T12:00:00", "2020-01-01T12:00:00")
    file_recs = opera.precip_rate.get(time_range=time_range)
    assert len(file_recs) == 1
    data = opera.precip_rate.open(file_recs[0])
    assert "precip_rate" in data.variables
