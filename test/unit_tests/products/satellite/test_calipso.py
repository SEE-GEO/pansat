"""
Tests for the pansat.products.satellite.calipso module.
"""
from datetime import datetime
import os
import numpy as np
import pytest
import pansat.products.satellite.calipso as calipso

TEST_NAMES = {"333mCLay": "CAL_LID_L2_333mCLay-ValStage1-V3-30.2016-11-21T10-41-52ZN.hdf"}
TEST_TIMES = {"333mCLay": datetime(2016, 11, 21, 10, 41, 52)}
PRODUCTS = [calipso.clay333m]
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
    Download Calipso 333mCLay file.
    """
    product = calipso.clay333m
    t_0 = datetime(2018, 6, 1, 10)
    t_1 = datetime(2018, 6, 1, 12)
    files = product.download(t_0, t_1)

    if HAS_HDF:
        product.open(files[0])
