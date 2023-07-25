from pathlib import Path
import numpy as np
import pytest

import conftest

from pansat.products.product_description import ProductDescription

HAS_HDF4 = False
try:
    import pyhdf
    from pansat.formats.hdf4 import HDF4File
    HAS_HDF4 = True
except Exception:
    pass

HAS_HDF5 = False
try:
    import pyhdf
    from pansat.formats.hdf5 import HDF5File
    HAS_HDF5 = True
except Exception:
    pass


@pytest.mark.parametrize("product_data", conftest.PRODUCT_DATA)
def test_read_product_description(product_data, request):
    """
    Reads product description test file and checks that the description
    attributes are parsed correctly.
    """
    product_data = request.getfixturevalue(product_data)
    description = ProductDescription(product_data / "product.ini")

    assert description.name == "test-description"

    assert len(description.dimensions) == 2
    assert description.dimensions["dimension_1"].name == "dimension_1"
    assert description.dimensions["dimension_2"].name == "dimension_2"

    assert len(description.coordinates) == 2
    assert description.coordinates["longitude"].name == "longitude"
    assert description.coordinates["latitude"].name == "latitude"

    assert len(description.attributes) == 1
    assert description.attributes["attribute_1"].name == "attribute_1"


@pytest.mark.skipif(not HAS_HDF4, reason="HDF4 library not available.")
def test_open_hdf4_product(hdf4_product_data):
    """
    Converts test file to xarray dataset.
    """
    product_data = hdf4_product_data
    description = ProductDescription(product_data / "product.ini")

    files = (product_data / "remote").glob("*.hdf")
    for path in files:
        file_handle = HDF4File(path)
        dataset = description.to_xarray_dataset(file_handle)

        lats = dataset.latitude.data
        assert np.all(lats > -10)
        assert np.all(lats < 10)

        lons, lats = description.load_lonlats(
            file_handle, slice(0, None, 10)
        )
        assert lons.size == 20
        assert lats.size == 20


@pytest.mark.skipif(not HAS_HDF5, reason="HDF4 library not available.")
def test_open_hdf5_product(hdf5_product_data):
    """
    Converts test file to xarray dataset.
    """
    product_data = hdf5_product_data
    description = ProductDescription(product_data / "product.ini")

    files = (product_data / "remote").glob("*.hdf")
    for path in files:
        file_handle = HDF5File(path)
        dataset = description.to_xarray_dataset(file_handle)

        lats = dataset.latitude.data
        assert np.all(lats > -10)
        assert np.all(lats < 10)

        lons, lats = description.load_lonlats(
            file_handle, slice(0, None, 10)
        )
        assert lons.size == 20
        assert lats.size == 20


@pytest.mark.skipif(not HAS_HDF4, reason="HDF4 library not available.")
def test_open_hdf4_product(hdf4_product_data):
    """
    Converts test file to xarray dataset.
    """
    product_data = hdf4_product_data
    description = ProductDescription(product_data / "product.ini")

    files = (product_data / "remote").glob("*.hdf")
    for path in files:
        file_handle = HDF4File(path)
        dataset = description.to_xarray_dataset(file_handle)

        lats = dataset.latitude.data
        assert np.all(lats > -10)
        assert np.all(lats < 10)

        slcs = {
            "dimension_1": slice(0, None, 10),
            "dimension_2": slice(0, None, 20)
        }
        lons, lats = description.load_lonlats(
            file_handle, slcs=slcs
        )
        assert lons.size == 20
        assert lats.size == 10


@pytest.mark.skipif(not HAS_HDF5, reason="HDF4 library not available.")
def test_open_hdf5_product(hdf5_product_data):
    """
    Converts test file to xarray dataset.
    """
    product_data = hdf5_product_data
    description = ProductDescription(product_data / "product.ini")

    files = (product_data / "remote").glob("*.hdf")
    for path in files:
        file_handle = HDF5File(path)
        dataset = description.to_xarray_dataset(file_handle)

        lats = dataset.latitude.data
        assert np.all(lats > -10)
        assert np.all(lats < 10)

        slcs = {
            "dimension_1": slice(0, None, 10),
            "dimension_2": slice(0, None, 20)
        }
        lons, lats = description.load_lonlats(
            file_handle, slcs=slcs
        )
        assert lons.size == 20
        assert lats.size == 10


@pytest.mark.parametrize("product_data", conftest.GRANULE_PRODUCT_DATA)
def test_read_granule_product_description(product_data, request):
    """
    Reads product description test file and checks that the description
    attributes are parsed correctly.
    """
    product_data = request.getfixturevalue(product_data)
    description = ProductDescription(product_data / "product.ini")

    assert description.name == "test-description"

    assert len(description.dimensions) == 2
    assert description.dimensions["scans"].name == "scans"
    assert description.dimensions["pixels"].name == "pixels"

    assert len(description.coordinates) == 3
    assert description.coordinates["longitude"].name == "longitude"
    assert description.coordinates["latitude"].name == "latitude"
    assert description.coordinates["time"].name == "time"


@pytest.mark.skipif(not HAS_HDF4, reason="HDF4 library not available.")
def test_get_granule_data_hdf4_product(hdf4_granule_product_data):
    """
    Tests the extract of granules from HDF4 files.
    """
    product_data = hdf4_granule_product_data
    description = ProductDescription(product_data / "product.ini")

    files = (product_data / "remote").glob("*.hdf")
    for path in files:
        file_handle = HDF4File(path)
        granule_data = description.get_granule_data(file_handle)

        assert len(granule_data) == 8


@pytest.mark.skipif(not HAS_HDF5, reason="HDF5 library not available.")
def test_get_granule_data_hdf5_product(hdf5_granule_product_data):
    """
    Tests the extract of granules from HDF4 files.
    """
    product_data = hdf5_granule_product_data
    description = ProductDescription(product_data / "product.ini")

    files = (product_data / "remote").glob("*.hdf")
    for path in files:
        file_handle = HDF5File(path)
        granule_data = description.get_granule_data(file_handle)

        assert len(granule_data) == 8
