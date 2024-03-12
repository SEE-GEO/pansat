"""
Tests for the pansat.products.satellite.cloudsat module.
"""
from datetime import datetime
import os
import numpy as np
import pytest
from pansat.products.satellite import cloudsat

TEST_NAMES = {
    "satellite.cloudsat.l1b_cpr": "2018143004115_64268_CS_1B-CPR_GRANULE_P_R05_E07_F00.hdf",
    "satellite.cloudsat.cstrack_cs_modis_aux": "CSTRACK_CS-MODIS-AUX_2012146193300_32326_V2-20.hdf"
}
TEST_TIMES = {
    "satellite.cloudsat.l1b_cpr": datetime(2018, 5, 23, 00, 41, 15),
    "satellite.cloudsat.cstrack_cs_modis_aux": datetime(2012, 5, 25, 19, 33)
}

PRODUCTS = [cloudsat.l1b_cpr, cloudsat.cstrack_modis_aux]
HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ

HAS_HDF = False
try:
    import pyhdf
    from pansat.formats.hdf4 import HDF4File

    HAS_HDF = True
except Exception:
    pass


@pytest.mark.parametrize("product", PRODUCTS)
def test_temporal_coverage(product):
    """
    Assert that time is correctly extracted from filename.
    """
    filename = TEST_NAMES[product.name]
    time_range = product.get_temporal_coverage(filename)
    print("TR :: ", time_range)
    assert time_range.start == TEST_TIMES[product.name]


@pytest.mark.parametrize("product", PRODUCTS)
def test_matches(product):
    """
    Assert that matches method returns true on the filename.
    """
    filename = TEST_NAMES[product.name]
    assert product.matches(filename)


@pytest.mark.slow
@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_download():
    """
    Download CloudSat L1B file.
    """
    product = cloudsat.l1b_cpr
    t_0 = datetime(2018, 6, 1, 10)
    t_1 = datetime(2018, 6, 1, 12)
    files = product.download(t_0, t_1)

    if HAS_HDF:
        product.open(files[0])


def test_cloud_class_masks():
    """
    Ensure that extraction of cloud properties from cloud scenario data works
    as expected.
    """
    data = np.array([2081])
    assert cloudsat._cloud_scenario_to_cloud_scenario_flag(data) == 1
    assert cloudsat._cloud_scenario_to_cloud_class(data) == 0
    assert cloudsat._cloud_scenario_to_land_sea_flag(data) == 1
    assert cloudsat._cloud_scenario_to_latitude_flag(data) == 0
    assert cloudsat._cloud_scenario_to_algorithm_flag(data) == 0
    assert cloudsat._cloud_scenario_to_quality_flag(data) == 1
    assert cloudsat._cloud_scenario_to_precipitation_flag(data) == 0


@pytest.mark.slow
@pytest.mark.skip(reason="Currently no way to test this.")
@pytest.mark.usefixtures("test_identities")
def test_download_rain_profile():
    """
    Download CloudSat rain profile.
    """
    product = cloudsat.l2c_rain_profile
    t_0 = datetime(2018, 6, 1, 10)
    t_1 = datetime(2018, 6, 1, 12)
    files = product.download(t_0, t_1)

    if HAS_HDF:
        product.open(files[0])
