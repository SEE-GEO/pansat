"""
Tests for the pansat.products.satellite.goes module.
"""
from datetime import datetime
import pytest

from pansat.file_record import FileRecord
from pansat.time import to_datetime, TimeRange
from pansat import geometry

from pansat.products.satellite.goes import (
    l1b_goes_16_rad_c01_full_disk,
    l1b_goes_16_rad_c02_full_disk,
    l1b_goes_16_rad_c03_full_disk,
    l1b_goes_16_rad_c04_full_disk,
    l1b_goes_16_rad_all_full_disk,
    l1b_goes_16_rad_c01_conus,
    l1b_goes_16_rad_c02_conus,
    l1b_goes_16_rad_c03_conus,
    l1b_goes_16_rad_c04_conus,
    l1b_goes_16_rad_all_conus,
)


PRODUCTS = [
    l1b_goes_16_rad_c01_full_disk,
    l1b_goes_16_rad_c02_full_disk,
    l1b_goes_16_rad_c03_full_disk,
    l1b_goes_16_rad_c04_full_disk,
    l1b_goes_16_rad_all_full_disk,
    l1b_goes_16_rad_c01_conus,
    l1b_goes_16_rad_c02_conus,
    l1b_goes_16_rad_c03_conus,
    l1b_goes_16_rad_c04_conus,
    l1b_goes_16_rad_all_conus,
]

FILENAMES = {
    l1b_goes_16_rad_c01_full_disk.name: (
        "OR_ABI-L1b-RadF-M3C01_G16_s201706"
        "20310085_e20170620320452_c20170620"
        "320492.nc"
    ),
    l1b_goes_16_rad_c02_full_disk.name: (
        "OR_ABI-L1b-RadF-M3C02_G16_s201706"
        "20310085_e20170620320452_c20170620"
        "320492.nc"
    ),
    l1b_goes_16_rad_c03_full_disk.name: (
        "OR_ABI-L1b-RadF-M3C03_G16_s201706"
        "20310085_e20170620320452_c20170620"
        "320492.nc"
    ),
    l1b_goes_16_rad_c04_full_disk.name: (
        "OR_ABI-L1b-RadF-M3C04_G16_s201706"
        "20310085_e20170620320452_c20170620"
        "320492.nc"
    ),
    l1b_goes_16_rad_all_full_disk.name: (
        "OR_ABI-L1b-RadF-M3C04_G16_s201706"
        "20310085_e20170620320452_c20170620"
        "320492.nc"
    ),
    l1b_goes_16_rad_c01_conus.name: (
        "OR_ABI-L1b-RadC-M3C01_G16_s201706"
        "20310085_e20170620320452_c20170620"
        "320492.nc"
    ),
    l1b_goes_16_rad_c02_conus.name: (
        "OR_ABI-L1b-RadC-M3C02_G16_s201706"
        "20310085_e20170620320452_c20170620"
        "320492.nc"
    ),
    l1b_goes_16_rad_c03_conus.name: (
        "OR_ABI-L1b-RadC-M3C03_G16_s201706"
        "20310085_e20170620320452_c20170620"
        "320492.nc"
    ),
    l1b_goes_16_rad_c04_conus.name: (
        "OR_ABI-L1b-RadC-M3C04_G16_s201706"
        "20310085_e20170620320452_c20170620"
        "320492.nc"
    ),
    l1b_goes_16_rad_all_conus.name: (
        "OR_ABI-L1b-RadC-M3C04_G16_s201706"
        "20310085_e20170620320452_c20170620"
        "320492.nc"
    ),
}


@pytest.mark.parametrize("product", PRODUCTS)
def test_filename_regexp(product):
    """
    Ensures that product matches filename.
    """
    rec = FileRecord(FILENAMES[product.name])
    assert product.matches(rec)


@pytest.mark.parametrize("product", PRODUCTS)
def test_get_temporal_coverage(product):
    """
    Ensures that filename to date yields the right date.
    """
    rec = FileRecord(FILENAMES[product.name])
    time_range = product.get_temporal_coverage(rec)
    date = to_datetime(time_range.start)
    assert date == datetime(2017, 3, 3, 3, 10, 8)

@pytest.mark.parametrize("product", PRODUCTS)
def test_get_spatial_coverage(product):
    rec = FileRecord(FILENAMES[product.name])
    geom = product.get_spatial_coverage(rec)
    assert isinstance(geom, geometry.Geometry)

@pytest.mark.slow
def test_download(tmp_path):
    """
    Ensures that downloading a single file works.
    """
    t_0 = datetime(2020, 1, 10, 0, 0)
    t_1 = datetime(2020, 1, 10, 0, 2)
    time_range = TimeRange(t_0, t_1)

    files = l1b_goes_16_rad_c01_conus.download(time_range, destination=tmp_path)
    assert len(files) > 0
    l1b_goes_16_rad_c01_conus.open(files[0])
