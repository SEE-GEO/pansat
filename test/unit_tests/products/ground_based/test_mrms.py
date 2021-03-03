from datetime import datetime

import pytest
from pansat.products.ground_based.mrms import mrms_precip_rate


def test_filename_to_date():
    """
    Ensure that filename to date extracts the right time from a given
    filename.
    """
    filename = "PrecipRate_00.00_20210101-020400.grib2.gz"
    time = mrms_precip_rate.filename_to_date(filename)
    assert time.year == 2021
    assert time.month == 1
    assert time.day == 1
    assert time.hour == 2
    assert time.minute == 4


@pytest.mark.xfail(reason="Requires cfgrib package.")
def test_download_and_open(tmp_path):
    """
    Ensure that the download method work as expected.
    """
    start_time = datetime(2021, 1, 1, 0, 0, 0)
    end_time = datetime(2021, 1, 1, 0, 0, 1)
    files = mrms_precip_rate.download(start_time, end_time, destination=tmp_path)
    assert len(files) == 1

    dataset = mrms_precip_rate.open(files[0])
