"""
Tests for the pansat.products.ground_based.amedas module.
=========================================================
"""
from datetime import datetime

from pansat.geometry import LineString
from pansat.products.ground_based import amedas


def test_amedas_product():
    """
    Test opening, recognintion, and parsing of time and spatial coverage
    for AMeDAS files.
    """
    filename = "Z__C_RJTD_20170101000000_SRF_GPV_Ggis1km_Prr60lv_ANAL_grib2.bin"


    assert amedas.precip_rate.matches(filename)

    time_range = amedas.precip_rate.get_temporal_coverage(filename)
    assert time_range.covers(datetime(2017, 1, 1, 0, 15))

    tokyo_to_kyoto = LineString([[139, 35], [135, 35]])
    domain = amedas.precip_rate.get_spatial_coverage(filename)
    assert domain.covers(tokyo_to_kyoto)
