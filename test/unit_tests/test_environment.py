"""
Tests for the pansat.environment module.
"""
from datetime import datetime
import os

import pytest

import pansat.config
from pansat.time import TimeRange
from pansat.download.providers.example import ExampleProvider
from pansat.products.example import hdf5_product
import pansat.environment as penv
import pansat.config as pconf


@pytest.fixture
def custom_data_dir(tmp_path_factory):
    """
    Sets up a pansat config with a temporary directory as data dir.
    """
    tmp_path = tmp_path_factory.mktemp("data_dir")
    pansat_dir = tmp_path / ".pansat"
    pansat_dir.mkdir()

    config_file = """
    [registry.test]
    path = '{tmp_path}'
    is_data_dir = true
    transparent = false
    """
    config_file = config_file.format(tmp_path=str(tmp_path))
    with open(pansat_dir / "config.toml", "w") as output_file:
        output_file.write(config_file)

    return tmp_path


def test_download_tracking(custom_data_dir, hdf5_product_data):
    """
    Ensure that registry tracks downloads.
    """
    os.chdir(custom_data_dir)
    pansat.config._CURRENT_CONFIG = None

    provider = ExampleProvider(hdf5_product_data, "hdf5")

    time_range = TimeRange(datetime(2020, 1, 1), datetime(2020, 1, 2))
    files = hdf5_product.download(time_range)

    assert files[0].local_path.parent.parent == custom_data_dir
    assert provider.counter == len(files)

    path = penv.lookup_file(files[0])
    assert path is not None

    files = hdf5_product.get(time_range)
    assert provider.counter == len(files)


@pytest.fixture
def custom_data_dir_with_index(tmp_path_factory, hdf5_product_data):
    """
    Sets up a pansat config with a temporary directory as data dir.
    """
    tmp_path = tmp_path_factory.mktemp("data_dir")
    pansat_dir = tmp_path / ".pansat"
    pansat_dir.mkdir()

    config_file = """
    [registry.test]
    path = '{tmp_path}'
    is_data_dir = true
    transparent = false
    """
    config_file = config_file.format(tmp_path=str(tmp_path))
    with open(pansat_dir / "config.toml", "w") as output_file:
        output_file.write(config_file)

    os.chdir(tmp_path)
    pansat.config._CURRENT_CONFIG = None

    provider = ExampleProvider(hdf5_product_data, "hdf5")
    files = hdf5_product
    time_range = TimeRange(datetime(2020, 1, 1, 0), datetime(2020, 1, 1, 1))
    hdf5_product.download(time_range)
    penv.save_registries()

    tmp_path_2 = tmp_path_factory.mktemp("data_dir_2")
    pansat_dir = tmp_path_2 / ".pansat"
    pansat_dir.mkdir()

    config_file = """
    [registry.test_2]
    path = '{tmp_path_2}'
    is_data_dir = true
    transparent = false

    [registry.test]
    path = '{tmp_path}'
    is_data_dir = true
    transparent = false
    """
    config_file = config_file.format(tmp_path=str(tmp_path), tmp_path_2=str(tmp_path_2))
    with open(pansat_dir / "config.toml", "w") as output_file:
        output_file.write(config_file)

    return tmp_path_2


def test_get_index(custom_data_dir_with_index, hdf5_product_data):
    """
    Ensure that the registry hierarchy is handlded correctly.
    """
    os.chdir(custom_data_dir_with_index)
    pansat.config._CURRENT_CONFIG = None

    index = penv.get_index(hdf5_product, recurrent=False)
    assert len(index) == 0

    provider = ExampleProvider(hdf5_product_data, "hdf5")
    time_range = TimeRange(datetime(2020, 1, 1, 1), datetime(2020, 1, 1, 3))
    files = hdf5_product.get(time_range=time_range)
    index = penv.get_index(hdf5_product)
    assert len(index) == 4
