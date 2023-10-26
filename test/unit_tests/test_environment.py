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
def custom_data_dir(tmp_path):
    """
    Sets up a pansat config with a temporary directory as data dir.
    """
    pansat_dir = tmp_path / ".pansat"
    pansat_dir.mkdir()

    config_file = """
    [registry.test]
    path = '{tmp_path}'
    is_data_dir = true
    """
    config_file = config_file.format(tmp_path=str(tmp_path))
    with open(pansat_dir / "config.toml", "w") as output_file:
        output_file.write(config_file)

    return tmp_path


def test_download_tracking(
        custom_data_dir,
        hdf5_product_data
):
    """
    Ensure that registry tracks downloads.
    """
    os.chdir(custom_data_dir)
    pansat.config._CURRENT_CONFIG = None

    provider = ExampleProvider(hdf5_product_data, "hdf5")

    files = hdf5_product
    time_range = TimeRange(
        datetime(2020, 1, 1),
        datetime(2020, 1, 2)
    )
    files = hdf5_product.download(time_range)

    assert files[0].local_path.parent == custom_data_dir

    path = penv.lookup_file(files[0])
    assert path is not None

    # Ensure that download are registered also in top registry.
    reg = pconf.get_current_config().registries[0]
    path = reg.find_local_path(files[0])
    assert path is not None
