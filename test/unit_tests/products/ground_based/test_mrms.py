from datetime import datetime

import pytest
from pansat.time import TimeRange
from pansat.products.ground_based.mrms import (
    precip_rate,
    precip_flag,
    radar_quality_index,
    precip_1h,
    precip_1h_gc,
    precip_1h_ms,
)
from pansat import FileRecord


_PRODUCTS = {
    "PrecipRate_00.00_20210101-020400.grib2.gz": precip_rate,
    "PrecipType_00.00_20210101-020400.grib2.gz": precip_flag,
    "RadarQualityIndex_00.00_20210101-020400.grib2.gz": radar_quality_index,
    "RadarOnly_QPE_01H_00.00_20210101-020400.grib2.gz": precip_1h,
    "GaugeCorr_QPE_01H_00.00_20210101-020400.grib2.gz": precip_1h_gc,
    "MultiSensor_QPE_01H_Pass2_00.00_20210101-020400.grib2.gz": precip_1h_ms,
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
    assert start_time.hour in [1, 2]
    assert start_time.minute in [3, 34]


@pytest.mark.slow
@pytest.mark.parametrize("filename", _PRODUCTS)
def test_download_and_open(tmp_path, filename):
    """
    Ensure that the download method work as expected.
    """
    product = _PRODUCTS[filename]
    start_time = datetime(2021, 1, 1, 0, 1, 1)
    end_time = datetime(2021, 1, 1, 0, 1, 2)
    time_range = TimeRange(start_time, end_time)
    files = product.download(time_range, destination=tmp_path)

    assert len(files) <= 1
    for rec in files:
        assert isinstance(rec, FileRecord)

    if len(files) > 1:
        dataset = product.open(files[0])
        assert product.variable_name in dataset
