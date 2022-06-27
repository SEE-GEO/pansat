"""
This file contains tests for the GOES AWS provider.
"""
from datetime import datetime, timedelta

from pansat.download.providers import GOESAWSProvider
from pansat.products.satellite.goes import (
    goes_16_l1b_radiances_c01_conus,
    goes_17_l1b_radiances_c01_conus,
)


def test_list_files():
    """
    Test that listing of files for given channel and day yields expected number
    of files (24 * 12 = 288).
    """
    provider = GOESAWSProvider(goes_16_l1b_radiances_c01_conus)
    files_16 = provider.get_files_by_day(2019, 77)
    assert len(files_16) == 288
    provider = GOESAWSProvider(goes_17_l1b_radiances_c01_conus)
    files_17 = provider.get_files_by_day(2019, 77)
    assert files_16 != files_17


def test_download(tmp_path):
    """
    Test that a file can be successfully downloaded.
    """
    provider = GOESAWSProvider(goes_16_l1b_radiances_c01_conus)

    assert str(goes_16_l1b_radiances_c01_conus) in provider.get_available_products()

    files = provider.get_files_by_day(2017, 77)
    provider.download_file(files[0], tmp_path / "test.nc")


def test_realtime(tmp_path):
    """
    Ensure that most recent files are found.
    """
    provider = GOESAWSProvider(goes_16_l1b_radiances_c01_conus)
    start_time = datetime.utcnow() - timedelta(minutes=15)
    end_time = datetime.utcnow()
    files = provider.get_files_in_range(start_time, end_time)
    assert len(files) > 0
