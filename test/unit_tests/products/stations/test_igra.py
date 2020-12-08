"""
Tests for the pansat.products.reanalysis.ncep module.
"""

from datetime import datetime
import os
import pytest
import pansat.products.stations.igra as igra
import random


PRODUCTS = [igra.IGRASoundings([30, 170])]


TEST_NAMES = {"igra-soundings": "ZZM00099027-data.txt.zip"}


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
@pytest.mark.usefixtures("test_identities")
def test_download(tmpdir):
    product = PRODUCTS[0]
    files = product.download(destination=str(tmpdir))
