"""
Tests for the pansat.catalog module.
"""

import pytest
import conftest

from pansat.catalog import Catalog
from pansat.products.example import (
    hdf5_product,
    hdf5_granule_product,
)


def test_catalog_from_existing_files(hdf5_product_data, hdf5_granule_product_data):
    """
    Test creation of catalog from existing files.
    """
    cat = Catalog.from_existing_files(hdf5_product_data)
    assert len(cat.indices) == 1
    assert list(cat.indices.keys())[0] == "example.hdf5_product"


def test_catalog_persistence(hdf5_product_data, hdf5_granule_product_data):
    """
    Test creation of catalog from existing files.
    """
    cat = Catalog.from_existing_files(hdf5_product_data)
    assert len(cat.indices) == 1
    assert list(cat.indices.keys())[0] == "example.hdf5_product"

    assert not (hdf5_product_data / ".pansat").exists()
    cat.save()
    assert (hdf5_product_data / ".pansat").exists()

    cat = Catalog(hdf5_product_data)
    assert len(cat.indices) == 1
    assert list(cat.indices.keys())[0] == "example.hdf5_product"
