"""
This file contains tests for the GOES AWS provider.
"""
from datetime import datetime, timedelta

import pytest
import xarray as xr

from pansat.download.providers.goes_aws import (
    goes_16_aws_provider,
    goes_17_aws_provider
)
from pansat.products.satellite.goes import (
    l1b_goes_16_rad_c01_conus,
    l1b_goes_17_rad_c01_conus,
)
from pansat.time import TimeRange


def test_list_files():
    """
    Test that listing of files for given channel and day yields expected number
    of files (24 * 12 = 288).
    """
    provider = goes_16_aws_provider
    files_16 = provider.find_files_by_day(
        l1b_goes_16_rad_c01_conus,
        datetime(2019, 3, 18)
    )
    assert len(files_16) == 288

    provider = goes_17_aws_provider
    files_17 = provider.find_files_by_day(
        l1b_goes_16_rad_c01_conus,
        datetime(2019, 3, 18)
    )
    assert files_16 != files_17


@pytest.mark.slow
def test_download(tmp_path):
    """
    Test that a file can be successfully downloaded.
    """
    provider = goes_16_aws_provider

    assert provider.provides(l1b_goes_16_rad_c01_conus)

    time = datetime(2019, 3, 18)
    time_range = TimeRange(time, time + timedelta(minutes=5))
    files = provider.find_files(l1b_goes_16_rad_c01_conus, time_range)
    file_rec = provider.download(files[0], tmp_path)
    print(file_rec.local_path)
    dataset = xr.load_dataset(file_rec.local_path)
    assert "Rad" in dataset


def test_realtime(tmp_path):
    """
    Ensure that most recent files are found.
    """
    provider = goes_17_aws_provider
    start_time = datetime.utcnow() - timedelta(minutes=15)
    end_time = datetime.utcnow()
    time_range = TimeRange(start_time, end_time)
    files = provider.find_files_in_range(
        l1b_goes_17_rad_c01_conus,
        time_range
    )
    assert len(files) > 0
