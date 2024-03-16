"""
Tests for the pansat.catalog module.
"""

import pytest
import conftest

from pansat.catalog import Catalog, Index
from pansat.file_record import FileRecord
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
    Test saving and loading of a catalog.
    """
    cat = Catalog.from_existing_files(hdf5_product_data)
    assert len(cat.indices) == 1
    assert list(cat.indices.keys())[0] == "example.hdf5_product"

    assert not (hdf5_product_data / ".pansat_catalog").exists()
    cat.save()
    assert (hdf5_product_data / ".pansat_catalog").exists()

    cat = Catalog(db_path=hdf5_product_data / ".pansat_catalog")
    assert len(cat.indices) == 1
    assert list(cat.indices.keys())[0] == "example.hdf5_product"


def test_catalog_consistency(
        hdf5_product_data,
        tmp_path
):
    """
    Ensure that file and index retrieval from two different catalog objects
    referring to the same database is consistent.
    """
    product_files = sorted(list(hdf5_product_data.glob("**/*.h5")))

    db_path = tmp_path / "cat"
    db_path.mkdir()
    index_1 = Index.index(hdf5_product, product_files[:2])
    cat_1 = Catalog(db_path=db_path, indices={hdf5_product_data.name: index_1})
    cat_1.save()
    index = cat_1.get_index(hdf5_product)
    assert len(index.data) == 2

    cat_2 = Catalog(db_path=db_path)
    rec = FileRecord(local_path=product_files[2], product=hdf5_product)
    cat_2.add(rec)

    path = cat_1.get_local_path(rec)
    assert path is not None

    index = cat_1.get_index(hdf5_product)
    assert len(index.data) == 3
