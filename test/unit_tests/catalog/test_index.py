"""
Tests for the pansat.catalog.index module.
"""
import pytest
import conftest

import numpy as np

from pansat.products.example import  (
    hdf5_product,
    hdf5_granule_product,
    thin_swath_product,
    write_thin_swath_product_data
)


from pansat.catalog import (
    Index,
    find_matches,
    matches_to_geopandas
)
from pansat.geometry import LonLatRect
from pansat.time import TimeRange


def test_indexing(hdf5_product_data):

    files = (hdf5_product_data / "remote").glob("*")

    index = Index.index(hdf5_product, files)

    roi = LonLatRect(0, 0, 5, 5)
    found = index.find(roi=roi)
    assert len(found) == 1

    roi = LonLatRect(0, 0, 50, 5)
    found = index.find(roi=roi)
    assert len(found) == 4

    roi = LonLatRect(0, 0, 50, 5)
    t_range = TimeRange("2020-01-01T00:00:00", "2020-01-01T01:00:59")
    found = index.find(time_range=t_range, roi=roi)
    assert len(found) == 2


def test_granule_indexing(hdf5_granule_product_data):

    files = (hdf5_granule_product_data / "remote").glob("*")

    index = Index.index(hdf5_granule_product, files)
    assert len(index.data) == 32

    roi = LonLatRect(0, 0, 5, 5)
    found = index.find(roi=roi)
    assert len(found) == 2

    roi = LonLatRect(0, 0, 2, 2)
    found = index.find(roi=roi)
    assert len(found) == 1

    t_range = TimeRange("2020-01-01T00:00:00", "2020-01-01T00:10:00")
    roi = LonLatRect(0, 0, 5, 5)
    found = index.find(time_range=t_range, roi=roi)
    assert len(found) == 1

    t_range = TimeRange("2020-01-01T00:00:00", "2020-01-01T00:10:00")
    roi = LonLatRect(5.001, 0, 10, 5)
    found = index.find(time_range=t_range, roi=roi)
    assert len(found) == 0


def test_save_and_load_index(tmp_path, hdf5_product_data):

    files = (hdf5_product_data / "remote").glob("*")
    index = Index.index(hdf5_product, files)

    index_name = index.save(tmp_path)
    assert len(list(tmp_path.glob("*.idx")))

    index_loaded = Index.load(tmp_path / index_name)

    roi = LonLatRect(0, 0, 5, 5)
    found = index_loaded.find(roi=roi)
    assert len(found) == 1

    roi = LonLatRect(0, 0, 50, 5)
    found = index_loaded.find(roi=roi)
    assert len(found) == 4

    roi = LonLatRect(0, 0, 50, 5)
    t_range = TimeRange("2020-01-01T00:00:00", "2020-01-01T01:00:00")
    found = index_loaded.find(time_range=t_range, roi=roi)
    assert len(found) == 2

def test_match_indices(tmp_path, hdf5_granule_product_data):

    files = (hdf5_granule_product_data / "remote").glob("*")
    index_1 = Index.index(hdf5_granule_product, files)

    path = tmp_path / "thin_swath"
    path.mkdir()
    write_thin_swath_product_data(tmp_path / "thin_swath")
    files = list(path.glob("*"))
    index_2 = Index.index(thin_swath_product, files)

    matches = find_matches(
        index_1,
        index_2,
        time_diff=np.timedelta64(60, "m"),
        merge=True
    )
    assert len(matches) == 2

    matches = find_matches(
        index_2,
        index_1,
        time_diff=np.timedelta64(60, "m"),
        merge=False
    )
    assert len(matches) == 10

    dframe_l, dframe_r = matches_to_geopandas(matches)
    assert dframe_l.shape[0] == dframe_r.shape[0]
