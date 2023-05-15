"""
Tests for the pansat.download.providers.eumetsat module.
"""
from datetime import datetime
import os

import pytest

from pansat.download.providers.eumetsat import EUMETSATProvider
from pansat.products.satellite.mhs import l1b_mhs


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.slow
def test_get_collections():
    """Test that accessing the collections works."""
    collections = EUMETSATProvider.get_collections()


def test_get_available_files():
    """Ensure that available MHS files are found."""
    start_time = datetime(2020, 1, 1)
    end_time = datetime(2020, 1, 2)
    provider = EUMETSATProvider(l1b_mhs)
    files = provider.get_files_in_range(start_time, end_time)
    assert len(files) > 0

    roi = (0, 0, 10, 10)
    files_roi = provider.get_files_in_range(
        start_time,
        end_time,
        bounding_box=roi
    )
    assert len(files_roi) > 0
    assert len(files_roi) < len(files)


@pytest.mark.slow
@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_get_download_file(tmp_path):
    """Ensure that available MHS files are found."""
    start_time = datetime(2020, 1, 1)
    end_time = datetime(2020, 1, 2)
    provider = EUMETSATProvider(l1b_mhs)
    files = provider.get_files_in_range(start_time, end_time)

    path = provider.download_file(files[0], tmp_path)
    assert path.exists()
