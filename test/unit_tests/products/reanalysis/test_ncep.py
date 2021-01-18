"""
Tests for the pansat.products.reanalysis.ncep module.
"""

from datetime import datetime
import os
import pytest
import pansat.products.reanalysis.ncep as ncep
import random
import sys


PRODUCTS = [
    ncep.NCEPReanalysis("vwnd.sig995", "surface"),
    ncep.NCEPReanalysis("air", "pressure"),
    ncep.NCEPReanalysis("air", "tropopause"),
]


TEST_NAMES = {
    "ncep.reanalysis-surface": "vwnd.sig995.2010.nc",
    "ncep.reanalysis-pressure": "air.1995.nc",
    "ncep.reanalysis-tropopause": "air.tropp.2018.nc",
}

TEST_TIMES = {
    "ncep.reanalysis-surface": datetime(2010, 1, 1, 0, 0),
    "ncep.reanalysis-pressure": datetime(1995, 1, 1, 0, 0),
    "ncep.reanalysis-tropopause": datetime(2018, 1, 1, 0, 0),
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
    tmp_dir = tmpdir_factory.mktemp(f"data{random.randint(1,300)}")
    return tmp_dir


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.skipif(sys.platform.startswith("win"), reason="Does not work on Windows")
@pytest.mark.usefixtures("test_identities")
def test_download(tmpdir):
    product = PRODUCTS[1]
    files = product.download(1995, 1999, str(tmpdir))
    # open file
    fn = tmpdir / TEST_NAMES[product.name]
    f = product.open(str(fn))
    f.close()
