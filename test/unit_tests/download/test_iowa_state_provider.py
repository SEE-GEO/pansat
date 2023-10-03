"""
Tests for the pansat.download.providers.iowa_state module.
"""
import numpy as np

from pansat.download.providers.iowa_state import IowaStateProvider
from pansat.products.ground_based.mrms import precip_rate


def test_find_files_by_day():
    """
    Ensure that get_files_by_day method returns files of the right day.
    """
    provider = IowaStateProvider()
    time = np.datetime64("2021-01-01T00:00:00")
    files = provider.find_files_by_day(
        precip_rate,
        time,
    )
    assert len(files) == 24 * 30


def test_download_file(tmp_path):
    """
    Ensure that downloading a file works as expected.
    """
    destination = tmp_path
    provider = IowaStateProvider()
    time = np.datetime64("2021-01-01T00:00:00")
    files = provider.find_files_by_day(precip_rate, time)
    rec = provider.download(files[0], destination)

    assert rec.local_path.exists()
    data = precip_rate.open(rec)
    assert "precip_rate" in data
