"""
Tests for the download.provides.nasa_pps module.
"""
import os
import pytest

from conftest import NEEDS_PANSAT_PASSWORD
import numpy as np

from pansat.download.providers.nasa_pps import NASAPPSProvider
from pansat.products.satellite.gpm import l1c_trmm_tmi


@NEEDS_PANSAT_PASSWORD
@pytest.mark.usefixtures("test_identities")
def test_find_files():
    """
    Ensure that procies finds GMI L1C files.
    """
    date = np.datetime64("2014-01-01")
    recs = l1c_trmm_tmi.find_files(date)
    assert 0 < len(recs)


@NEEDS_PANSAT_PASSWORD
@pytest.mark.usefixtures("test_identities")
@pytest.mark.slow
def test_download_file(tmp_path):
    """
    Ensure that procies finds GMI L1C files.
    """
    date = np.datetime64("2014-01-01")
    recs = l1c_trmm_tmi.get(date, destination=tmp_path)
    assert 0 < len(recs)
