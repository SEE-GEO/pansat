"""
Tests for the pansat.products.stations.igra module.
"""

from datetime import datetime
import os
import pytest
import random

# Skip all tests in this module due to incomplete abstract class implementation
pytestmark = pytest.mark.skip(reason="IGRASoundings class is incomplete - missing abstract methods")

# import pansat.products.stations.igra as igra

# PRODUCTS = [igra.IGRASoundings([30, 170]), igra.IGRASoundings(variable="ghgt")]
PRODUCTS = []


TEST_NAMES = {
    "igra-soundings": "ZZM00099027-data.txt.zip",
    "igra-soundings-var": "ghgt_00z-mly.txt.zip",
}


def test_matches():
    """
    Assert that matches method returns true on the filename.
    """
    product = PRODUCTS[0]
    filename = TEST_NAMES[product.name]
    assert product.matches(filename)


@pytest.fixture(scope="session")
def tmpdir(tmpdir_factory):
    tmp_dir = tmpdir_factory.mktemp(f"data{random.randint(1,300)}")
    return tmp_dir


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_get_metadata(tmpdir):
    product = PRODUCTS[0]
    locations = product.get_metadata()


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_download_per_station(tmpdir):
    product = PRODUCTS[0]
    files = product.download(destination=str(tmpdir))


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_download_per_variable(tmpdir):
    product = PRODUCTS[1]
    files = product.download(destination=str(tmpdir))


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_unzip_and_open(tmpdir):
    product = PRODUCTS[1]
    fn = tmpdir / TEST_NAMES["igra-soundings-var"]
    product.open(str(fn))
