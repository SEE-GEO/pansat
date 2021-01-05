"""
Tests for the pansat.products.satellite.goes module.
"""
from datetime import datetime
import pytest
from pansat.products.satellite.goes import (goes_16_l1b_radiances_c01_full_disk,
                                            goes_16_l1b_radiances_c02_full_disk,
                                            goes_16_l1b_radiances_c03_full_disk,
                                            goes_16_l1b_radiances_c04_full_disk,
                                            goes_16_l1b_radiances_all_full_disk,
                                            goes_16_l1b_radiances_c01_conus,
                                            goes_16_l1b_radiances_c02_conus,
                                            goes_16_l1b_radiances_c03_conus,
                                            goes_16_l1b_radiances_c04_conus,
                                            goes_16_l1b_radiances_all_conus)

def _id(product):
    return str(product) + f"_{product.channel}_{product.region}"

PRODUCTS = [
    goes_16_l1b_radiances_c01_full_disk,
    goes_16_l1b_radiances_c02_full_disk,
    goes_16_l1b_radiances_c03_full_disk,
    goes_16_l1b_radiances_c04_full_disk,
    goes_16_l1b_radiances_all_full_disk,
    goes_16_l1b_radiances_c01_conus,
    goes_16_l1b_radiances_c02_conus,
    goes_16_l1b_radiances_c03_conus,
    goes_16_l1b_radiances_c04_conus,
    goes_16_l1b_radiances_all_conus
]

FILENAMES = {
    _id(goes_16_l1b_radiances_c01_full_disk): ("OR_ABI-L1b-RadF-M3C01_G16_s201706"
                                               "20310085_e20170620320452_c20170620"
                                               "320492.nc"),
    _id(goes_16_l1b_radiances_c02_full_disk): ("OR_ABI-L1b-RadF-M3C02_G16_s201706"
                                               "20310085_e20170620320452_c20170620"
                                               "320492.nc"),
    _id(goes_16_l1b_radiances_c03_full_disk): ("OR_ABI-L1b-RadF-M3C03_G16_s201706"
                                               "20310085_e20170620320452_c20170620"
                                               "320492.nc"),
    _id(goes_16_l1b_radiances_c04_full_disk): ("OR_ABI-L1b-RadF-M3C04_G16_s201706"
                                               "20310085_e20170620320452_c20170620"
                                               "320492.nc"),
    _id(goes_16_l1b_radiances_all_full_disk): ("OR_ABI-L1b-RadF-M3C04_G16_s201706"
                                               "20310085_e20170620320452_c20170620"
                                               "320492.nc"),
    _id(goes_16_l1b_radiances_c01_conus): ("OR_ABI-L1b-RadC-M3C01_G16_s201706"
                                           "20310085_e20170620320452_c20170620"
                                           "320492.nc"),
    _id(goes_16_l1b_radiances_c02_conus): ("OR_ABI-L1b-RadC-M3C02_G16_s201706"
                                           "20310085_e20170620320452_c20170620"
                                           "320492.nc"),
    _id(goes_16_l1b_radiances_c03_conus): ("OR_ABI-L1b-RadC-M3C03_G16_s201706"
                                           "20310085_e20170620320452_c20170620"
                                           "320492.nc"),
    _id(goes_16_l1b_radiances_c04_conus): ("OR_ABI-L1b-RadC-M3C04_G16_s201706"
                                           "20310085_e20170620320452_c20170620"
                                           "320492.nc"),
    _id(goes_16_l1b_radiances_all_conus): ("OR_ABI-L1b-RadC-M3C04_G16_s201706"
                                           "20310085_e20170620320452_c20170620"
                                           "320492.nc")
}


@pytest.mark.parametrize("product", PRODUCTS)
def test_filename_regexp(product):
    """
    Ensures that product matches filename.
    """
    filename = FILENAMES[_id(product)]
    assert product.matches(filename)


@pytest.mark.parametrize("product", PRODUCTS)
def test_filename_to_date(product):
    """
    Ensures that filename to date yields the right date.
    """
    filename = FILENAMES[_id(product)]
    date = product.filename_to_date(filename)
    assert date == datetime(2017, 3, 3, 3, 10, 8)
