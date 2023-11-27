"""
Tests for the pansat.download.providers.eumetsat module.
"""
from datetime import datetime
import os

import pytest

from pansat import TimeRange
from pansat.geometry import LonLatRect
from pansat.download.providers.eumetsat import eumetsat_provider
from pansat.products.satellite.meteosat import l1b_msg_seviri


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.slow
def test_get_collections():
    """Test that accessing the collections works."""
    collections = eumetsat_provider.get_collections()


def test_find_files():
    """Ensure that available SEVIRI files are found."""
    start_time = datetime(2020, 1, 1)
    end_time = datetime(2020, 1, 2)
    time_range = TimeRange(start_time, end_time)
    provider = eumetsat_provider
    files = provider.find_files(l1b_msg_seviri, time_range)
    assert len(files) > 0

    roi = LonLatRect(0, 0, 10, 10)
    files_roi = provider.find_files(
        l1b_msg_seviri,
        time_range,
        roi=roi
    )
    assert len(files_roi) > 0

    roi = LonLatRect(160, 0, 180, 10)
    files_roi = provider.find_files(
        l1b_msg_seviri,
        time_range,
        roi=roi
    )
    assert len(files_roi) == 0


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
