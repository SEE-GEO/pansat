"""
Test for NASA GES DISC provider.
"""
import datetime
import os

from conftest import NEEDS_PANSAT_PASSWORD

import numpy as np
import pytest

from pansat import FileRecord, TimeRange
from pansat.download.providers.meteo_france import meteo_france_partner_provider
from pansat.products.ground_based.opera import precip_rate


@NEEDS_PANSAT_PASSWORD
@pytest.mark.usefixtures("test_identities")
def test_find_files():
    """
    Ensure that provider finds a single file available for a given day.
    """
    time = np.datetime64("2020-01-01T00:00:00")
    files = meteo_france_partner_provider.find_files_by_day(precip_rate, time)
    assert len(files) == 1


def test_provides():
    """
    Ensure that the data provider provides OPERA products.
    """
    assert meteo_france_partner_provider.provides(precip_rate)


@NEEDS_PANSAT_PASSWORD
@pytest.mark.usefixtures("test_identities")
@pytest.mark.slow
def test_download(tmp_path):
    """
    Ensure that the data provider correctly downloads OPERA data.
    """
    time = np.datetime64("2020-01-01T00:00:00")
    files = meteo_france_partner_provider.find_files_by_day(precip_rate, time)
    assert len(files) == 1

    files[0].download(destination=tmp_path)
    assert (tmp_path / precip_rate.get_filename(time)).exists()
