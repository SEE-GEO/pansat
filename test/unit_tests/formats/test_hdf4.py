"""
Tests for the ``pansat.formats.hdf4`` module.
"""
from pathlib import Path, PurePath
from pansat.formats.hdf4 import HDF4File

TEST_FILE = PurePath(__file__).parent / "test_data" / "test_file.hdf"


def test_dataset():
    """
    Reads the test file and ensures that  the dataset (DS API)
    is loaded correctly.
    """
    file = HDF4File(TEST_FILE)

    print(file.datasets)
    dataset = file.dataset_1
    assert dataset.name == "dataset_1"
    assert dataset.shape == (1, 10)


def test_vdata():
    """
    Reads the test file and ensures that both the vdata (VS API)
    is loaded correctly.
    """
    file = HDF4File(TEST_FILE)

    print(file.datasets)
    vdata = file.vdata_1
    assert vdata.name == "vdata_1"
    assert vdata.n_fields == 10
