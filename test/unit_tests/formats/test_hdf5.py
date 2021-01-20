"""
Tests for the ``pansat.formats.hdf4`` module.
"""
from pathlib import Path, PurePath
import numpy as np

import pytest

HAS_HDF = False
try:
    from pansat.formats.hdf5 import HDF5File

    HAS_HDF = True
except Exception:
    pass

TEST_FILE = PurePath(__file__).parent / "test_data" / "test_file.hdf5"


@pytest.mark.skipif(not HAS_HDF, reason="h5py not available.")
def test_dataset():
    """
    Reads the test file and ensures that  the dataset (DS API)
    is loaded correctly.
    """
    file = HDF5File(TEST_FILE, "r")
    data = file.data[:]
    assert np.all(np.isclose(data, np.arange(100)))
