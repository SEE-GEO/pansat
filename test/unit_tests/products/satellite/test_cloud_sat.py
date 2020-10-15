"""
Tests for the pansat.products.satellite.cloudsat module.
"""
import pytest
import pansat.products.satellite.cloud_sat as cloud_sat
from datetime import datetime

TEST_NAMES = {
    "1B-CPR": "2018143004115_64268_CS_1B-CPR_GRANULE_P_R05_E07_F00.hdf"
    }

TEST_TIMES = {
    "1B-CPR": datetime(2018, 5, 23, 00, 41, 15)
}

products = [cloud_sat.l1b_cpr]

@pytest.mark.parametrize("product", products)
def test_filename_to_date(product):
    """
    Assert that time is correctly extracted from filename.
    """
    filename = TEST_NAMES[product.name]
    time = product.filename_to_date(filename)
    assert time == TEST_TIMES[product.name]

@pytest.mark.parametrize("product", products)
def test_matches(product):
    """
    Assert that matches method returns true on the filename.
    """
    filename = TEST_NAMES[product.name]
    assert product.matches(filename)

@pytest.mark.usefixtures("test_identities")
def test_download():
    product = cloud_sat.l1b_cpr
    t_0 = datetime(2018, 6, 1, 10)
    t_1 = datetime(2018, 6, 1, 12)
    product.download(t_0, t_1)
