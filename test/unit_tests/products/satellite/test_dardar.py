"""
Tests for the pansat.products.satellite.dardar module.
"""
from datetime import datetime
import os
import numpy as np
import pytest
import pansat.products.satellite.dardar as dardar

TEST_NAMES = {"DARDAR_CLOUD": "DARDAR-CLOUD_v2.1.1_2009001035423_14254.hdf"}
TEST_TIMES = {"DARDAR_CLOUD": datetime(2009, 1, 1, 3, 54, 23)}
PRODUCTS = [dardar.dardar_cloud]
HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ

HAS_HDF = False
try:
    import pyhdf
    from pansat.formats.hdf4 import HDF4File

    HAS_HDF = True
except Exception:
    pass


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
    Download Dardar-cloud file.
    """
    product = dardar.dardar_cloud
    t_0 = datetime(2018, 6, 1, 10)
    t_1 = datetime(2018, 6, 1, 12)
    files = product.download(t_0, t_1)

    if HAS_HDF:
        product.open(files[0])
