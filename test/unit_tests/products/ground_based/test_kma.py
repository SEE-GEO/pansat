"""
Tests for the KMA products defined in pansat.products.ground_based.kma.
"""
import numpy as np


from pansat.geometry import Point
from pansat.products.ground_based.kma import precip_rate


def test_match_filename():
    """
    Assert that file timestamp is parsed correctly.
    """
    filename = "AWS_Interp_Resol1km_aug1_QC0_202210072230.nc"
    assert precip_rate.matches(filename)


def test_get_temporal_coverage():
    """
    Assert that file timestamp is parsed correctly.
    """
    filename = "/data/korea/202210/07/AWS_Interp_Resol1km_aug1_QC0_202210072230.nc"
    time_range = precip_rate.get_temporal_coverage(filename)
    assert time_range.covers(np.datetime64("2022-10-07T13:35:00"))
    assert not time_range.covers(np.datetime64("2022-10-07T13:45:00"))


def test_get_spatial_coverage():
    """
    Assert that file timestamp is parsed correctly.
    """
    filename = "/data/korea/202210/07/AWS_Interp_Resol1km_aug1_QC0_202210072230.nc"
    domain = precip_rate.get_spatial_coverage(filename)
    seol = Point(126.59, 37.34)
    assert domain.covers(seol)
    tokyo = Point(139.46, 35.41)
    assert not domain.covers(tokyo)
