"""
This file contains tests for the discrete provider base class.
"""
import os
import pytest

from datetime import datetime
from pansat.download.providers import IcareProvider
from pansat.products.satellite.modis import modis_terra_1km
from pansat.products.satellite.dardar import dardar_cloud


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_files_in_range():
    """
    Test that the expected number of files in given range is
    returned.

    - Checks that list is sorted by start time.
    - Checks that only files starting after the start time are
      returned when start_inclusive is false.
    - Checks that right file is included when start_inclusive is true.
    - Checks that this works across day boundaries.
    """
    provider = IcareProvider(modis_terra_1km)
    t0 = datetime(2018, 1, 14, 0, 42)
    t1 = datetime(2018, 1, 14, 0, 52)

    files = provider.get_files_in_range(t0, t1)
    assert modis_terra_1km.filename_to_date(files[0]).minute > 42
    assert len(files) == 2

    files_sorted = sorted(files, key=modis_terra_1km.filename_to_date)
    assert files_sorted == files

    files = provider.get_files_in_range(t0, t1, True)
    assert len(files) == 3
    assert modis_terra_1km.filename_to_date(files[0]).minute == 40

    t0 = datetime(2018, 1, 14, 0, 1)
    t1 = datetime(2018, 1, 14, 0, 2)
    files = provider.get_files_in_range(t0, t1, False)
    assert len(files) == 0

    t0 = datetime(2018, 1, 14, 0, 1)
    t1 = datetime(2018, 1, 14, 0, 2)
    files = provider.get_files_in_range(t0, t1, True)
    assert len(files) == 1

    t0 = datetime(2018, 1, 13, 23, 59)
    t1 = datetime(2018, 1, 14, 0, 6)
    files = provider.get_files_in_range(t0, t1, False)
    assert len(files) == 3


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_get_file_by_date():
    """
    Test that get file by date returns the closes file in time that starts
    before the given time
    """
    provider = IcareProvider(modis_terra_1km)

    t = datetime(2018, 1, 14, 0, 42)
    file = provider.get_file_by_date(t)
    assert modis_terra_1km.filename_to_date(file).minute == 40

    t = datetime(2018, 1, 14, 0, 0, 0)
    file = provider.get_file_by_date(t)
    assert modis_terra_1km.filename_to_date(file).minute == 0


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_last_file_of_day_included():
    """
    This test ensure that the last file of each day is included. This test
    covers a bug reported, which caused the discrete provider to miss the
    last file of each day.
    """
    from pansat.download.providers import IcareProvider
    from datetime import datetime
    provider = IcareProvider(dardar_cloud)
    t_0 = datetime(2006, 6, 20, 22, 30)
    t_1 = datetime(2006, 6, 21, 0, 0)
    files = provider.get_files_in_range(t_0, t_1)
    assert len(files) == 1

