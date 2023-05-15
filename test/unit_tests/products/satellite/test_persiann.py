"""
Tests for the available PERSIANN products.
"""
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

from pansat.products.satellite.persiann import CCS, PDIRNow


@pytest.mask.slow
def test_ccs(tmp_path):
    """
    Test downloading and opening of PERSIANN CCS files.
    """
    day = np.random.randint(1, 31)
    start = datetime(2020, 12, day)
    product = CCS(1)
    files = product.download(start, start, tmp_path / "CCS")

    assert Path(files[0]).exists()

    data = product.open(files[0])
    assert "latitude" in data.dims
    assert "longitude" in data.dims


@pytest.mask.slow
def test_pdirnow(tmp_path):
    """
    Test downloading and opening of PERSIANN CCS files.
    """
    day = np.random.randint(1, 31)
    start = datetime(2020, 12, day)
    product = PDIRNow(1)
    files = product.download(start, start, tmp_path / "PDIRNow")

    assert Path(files[0]).exists()

    data = product.open(files[0])
    assert "latitude" in data.dims
    assert "longitude" in data.dims
