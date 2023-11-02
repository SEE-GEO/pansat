"""
Contains fixtures that are automatically available in all test files.
"""
from datetime import datetime, timedelta
from pathlib import PurePath, Path

import numpy as np
import pytest

from pansat.products.example import (
    write_hdf4_product_data,
    write_hdf5_product_data,
    write_hdf4_granule_product_data,
    write_hdf5_granule_product_data,
    EXAMPLE_PRODUCT_DESCRIPTION,
    EXAMPLE_GRANULE_PRODUCT_DESCRIPTION,
)
from pansat.download.providers.example import ExampleProvider
from pansat.products.example import write_hdf5_product_data
from pansat.config import get_current_config


HAS_HDF4 = False
try:
    from pyhdf.SD import SD, SDC

    HAS_HDF4 = True
except ImportError:
    pass


HAS_HDF5 = False
try:
    from h5py import File

    HAS_HDF5 = True
except ImportError:
    HAS_HDF5 = False


@pytest.fixture()
def test_identities(monkeypatch):
    """
    Fixture that makes all tests use the test identities file that contains the
    test login data for data providers.
    """
    test_identity_file = Path(
        PurePath(__file__).parent / "test_data" / "identities.json"
    )
    config = get_current_config()
    config.identity_file = test_identity_file
    monkeypatch.setattr("pansat.download.accounts._PANSAT_SECRET", None)
    import pansat.download.accounts as accs

    accs.parse_identity_file()


@pytest.fixture()
def product_description(tmp_path):
    """
    Populates a temporary path with a product description file.
    """
    with open(tmp_path / "product.ini", "w") as descr:
        descr.write(EXAMPLE_PRODUCT_DESCRIPTION)
    yield tmp_path


@pytest.fixture()
def hdf4_product_data(product_description):
    """
    Populates a temporary directory with a product description and test
    files in HDF4 format.
    """
    tmp_path = product_description
    remote_path = tmp_path / "remote"
    remote_path.mkdir(exist_ok=True)
    write_hdf4_product_data(remote_path)
    yield tmp_path


@pytest.fixture()
def hdf5_product_data(product_description):
    """
    Populates a temporary directory with a product description and test
    files in HDF5 format.
    """
    tmp_path = product_description
    remote_path = tmp_path / "remote"
    remote_path.mkdir(exist_ok=True)
    write_hdf5_product_data(remote_path)
    yield tmp_path


@pytest.fixture()
def hdf5_product_provider(hdf5_product_data):
    """
    A provider providing hdf5 product data.
    """
    provider = ExampleProvider(hdf5_product_data, "hdf5")
    return provider


PRODUCT_DATA = [
    pytest.param(
        "hdf4_product_data",
        marks=pytest.mark.skipif(not HAS_HDF4, reason="HDF4 library not available."),
    ),
    pytest.param(
        "hdf5_product_data",
        marks=pytest.mark.skipif(not HAS_HDF5, reason="HDF5 library not available."),
    ),
]


@pytest.fixture()
def granule_product_description(tmp_path):
    """
    Populates a temporary path with the granule product description file.
    """
    with open(tmp_path / "product.ini", "w") as descr:
        descr.write(EXAMPLE_GRANULE_PRODUCT_DESCRIPTION)
    yield tmp_path


@pytest.fixture()
def hdf4_granule_product_data(granule_product_description):
    """
    Populates a temporary directory with a product description and test
    files in HDF4 format.
    """
    tmp_path = granule_product_description
    remote_path = tmp_path / "remote"
    remote_path.mkdir(exist_ok=True)
    write_hdf4_granule_product_data(remote_path)
    yield tmp_path


@pytest.fixture()
def hdf5_granule_product_data(granule_product_description):
    """
    Populates a temporary directory with the granule product descriptionu
    and test files in HDF5 format.
    """
    tmp_path = granule_product_description
    remote_path = tmp_path / "remote"
    remote_path.mkdir(exist_ok=True)
    write_hdf5_granule_product_data(remote_path)
    yield tmp_path


GRANULE_PRODUCT_DATA = [
    pytest.param(
        "hdf4_granule_product_data",
        marks=pytest.mark.skipif(not HAS_HDF4, reason="HDF4 library not available."),
    ),
    pytest.param(
        "hdf5_granule_product_data",
        marks=pytest.mark.skipif(not HAS_HDF5, reason="HDF5 library not available."),
    ),
]
