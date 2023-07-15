"""
Tests for the pansat.catalog.index module.
"""
import pytest
import conftest

from pansat.products.example import  hdf5_product
from pansat.catalog import Index
from pansat.geometry import LonLatRect
from pansat.time import TimeRange


def test_indexing(hdf5_product_data):

    files = (hdf5_product_data / "remote").glob("*")

    index = Index.index(hdf5_product, files)

    roi = LonLatRect(0, 0, 5, 5)
    found = index.find_files(roi=roi)
    assert len(found) == 1

    roi = LonLatRect(0, 0, 50, 5)
    found = index.find_files(roi=roi)
    assert len(found) == 4

    roi = LonLatRect(0, 0, 50, 5)
    t_range = TimeRange("2020-01-01T00:00:00", "2020-01-01T01:00:00")
    found = index.find_files(time_range=t_range, roi=roi)
    assert len(found) == 2

def test_save_and_load_index(tmp_path, hdf5_product_data):

    files = (hdf5_product_data / "remote").glob("*")
    index = Index.index(hdf5_product, files)

    index_name = index.save(tmp_path)
    assert len(list(tmp_path.glob("*.idx")))

    index_loaded = Index.load(tmp_path / index_name)

    roi = LonLatRect(0, 0, 5, 5)
    found = index_loaded.find_files(roi=roi)
    assert len(found) == 1

    roi = LonLatRect(0, 0, 50, 5)
    found = index_loaded.find_files(roi=roi)
    assert len(found) == 4

    roi = LonLatRect(0, 0, 50, 5)
    t_range = TimeRange("2020-01-01T00:00:00", "2020-01-01T01:00:00")
    found = index_loaded.find_files(time_range=t_range, roi=roi)
    assert len(found) == 2
