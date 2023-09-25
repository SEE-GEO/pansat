from datetime import datetime

import pytest
from pansat.time import TimeRange
from pansat.products.ground_based.mrms import (
    mrms_precip_rate,
    mrms_precip_flag,
    mrms_radar_quality_index,
)

_PRODUCTS = {
    "PrecipRate_00.00_20210101-020400.grib2.gz": mrms_precip_rate,
    "PrecipType_00.00_20210101-020400.grib2.gz": mrms_precip_flag,
    "RadarQualityIndex_00.00_20210101-020400.grib2.gz": mrms_radar_quality_index,
}


@pytest.mark.parametrize("filename", _PRODUCTS)
def test_filename_to_date(filename):
    """
    Ensure that filename to date extracts the right time from a given
    filename.
    """
    product = _PRODUCTS[filename]
    time = product.filename_to_date(filename)
    assert time.year == 2021
    assert time.month == 1
    assert time.day == 1
    assert time.hour == 2
    assert time.minute == 4


@pytest.mark.slow
@pytest.mark.parametrize("filename", _PRODUCTS)
def test_download_and_open(tmp_path, filename):
    """
    Ensure that the download method work as expected.
    """
    product = _PRODUCTS[filename]
    start_time = datetime(2021, 1, 1, 0, 1, 0)
    end_time = datetime(2021, 1, 1, 0, 1, 1)
    time_range = TimeRange(start_time, end_time)
    files = product.download(product, time_range, destination=tmp_path)
    assert len(files) == 1

    dataset = mrms_precip_rate.open(files[0])
