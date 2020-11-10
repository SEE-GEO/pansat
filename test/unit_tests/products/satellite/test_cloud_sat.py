"""
Tests for the pansat.products.satellite.cloudsat module.
"""
from datetime import datetime
import os
import numpy as np
import pytest
import pansat.products.satellite.cloud_sat as cloud_sat

TEST_NAMES = {"1B-CPR": "2018143004115_64268_CS_1B-CPR_GRANULE_P_R05_E07_F00.hdf"}
TEST_TIMES = {"1B-CPR": datetime(2018, 5, 23, 00, 41, 15)}
PRODUCTS = [cloud_sat.l1b_cpr]
HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


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

@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_download():
    """
    Download CloudSat L1B file.
    """
    product = cloud_sat.l1b_cpr
    t_0 = datetime(2018, 6, 1, 10)
    t_1 = datetime(2018, 6, 1, 12)
    product.download(t_0, t_1)


def test_cloud_class_masks():
    """
    Ensure that extraction of cloud properties from cloud scenario data works
    as expected.
    """
    data = np.array([2081])
    assert cloud_sat._cloud_scenario_to_cloud_scenario_flag(data) == 1
    assert cloud_sat._cloud_scenario_to_cloud_class(data) == 0
    assert cloud_sat._cloud_scenario_to_land_sea_flag(data) == 1
    assert cloud_sat._cloud_scenario_to_latitude_flag(data) == 0
    assert cloud_sat._cloud_scenario_to_algorithm_flag(data) == 0
    assert cloud_sat._cloud_scenario_to_quality_flag(data) == 1
    assert cloud_sat._cloud_scenario_to_precipitation_flag(data) == 0
