from datetime import datetime

import pytest
from pansat.time import TimeRange
from pansat.products.ground_based.mrms import (
    precip_rate,
    precip_flag,
    radar_quality_index,
)
from pansat import FileRecord


_PRODUCTS = {
    "PrecipRate_00.00_20210101-020400.grib2.gz": precip_rate,
    "PrecipType_00.00_20210101-020400.grib2.gz": precip_flag,
    "RadarQualityIndex_00.00_20210101-020400.grib2.gz": radar_quality_index,
}


@pytest.mark.parametrize("filename", _PRODUCTS)
def test_filename_to_date(filename):
    """
    Ensure that filename to date extracts the right time from a given
    filename.
    """
    product = _PRODUCTS[filename]
    time_range = product.get_temporal_coverage(filename)
    start_time = time_range.start

    assert start_time.year == 2021
    assert start_time.month == 1
    assert start_time.day == 1
    assert start_time.hour == 2
    assert start_time.minute == 4


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
    files = product.download(time_range, destination=tmp_path)

    assert len(files) == 1
    for rec in files:
        assert isinstance(rec, FileRecord)

    dataset = product.open(files[0])
    assert product.variable_name in dataset
