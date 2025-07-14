"""
Tests for the Stage IV products defined in pansat.products.ground_based.stage4.
"""
import numpy as np


from pansat.geometry import Point
from pansat.products.ground_based.stage4 import surface_precip


def test_match_filename():
    """
    Assert that file timestamp is parsed correctly.
    """
    filename = "stage4.200309.tar"
    assert surface_precip.matches(filename)


def test_get_temporal_coverage():
    """
    Assert that file timestamp is parsed correctly.
    """
    filename = "stage4.200309.tar"
    time_range = surface_precip.get_temporal_coverage(filename)
    assert time_range.covers(np.datetime64("2003-09-01"))
    assert not time_range.covers(np.datetime64("2003-08-31"))
    assert not time_range.covers(np.datetime64("2003-10-01T00:00:01"))


def test_get_spatial_coverage():
    """
    Assert that file timestamp is parsed correctly.
    """
    filename = "stage4.200309.tar"
    domain = surface_precip.get_spatial_coverage(filename)
    new_york = Point(-74.0060, 40.7128)
    denver = Point(-104.9903, 39.7392)
    tokyo = Point(139.46, 35.41)
    assert domain.covers(new_york)
    assert domain.covers(denver)
    assert not domain.covers(tokyo)


def test_open_file():
    """
    Assert that file timestamp is parsed correctly.
    """
    recs = surface_precip.get("2003-01-02")
    data = surface_precip.open(recs[0])
    assert "surface_precip" in data
