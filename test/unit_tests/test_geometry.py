"""
Tests for the pansat.geometry module.
"""
import numpy as np

from pansat.geometry import (
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
    lonlats_to_polygon
)


def test_line_string():
    """
    Ensure the intersecting line strings cover each other.
    """
    ls_1 = LineString([(0, 0), (10, 0), (20, 0), (30, 0)])
    ls_2 = LineString([(20, -20), (20, -10), (20, 0), (20, 10)])
    assert ls_1.covers(ls_2)

    ls_3 = ls_1.merge(ls_2)
    assert ls_3.covers(ls_2)

    lon_min, lat_min, lon_max, lat_max = ls_1.bounding_box_corners
    assert lon_min == 0
    assert lon_max == 30
    assert lat_min == 0
    assert lat_max == 0


def test_multi_line_string():
    """
    Ensure the intersecting line strings cover each other.
    """
    ls_1 = MultiLineString([[(0, 0), (10, 0)], [(30, 0), (50, 0)]])
    ls_2 = LineString([(20, -20), (20, -10), (20, 0), (20, 10)])
    assert not ls_1.covers(ls_2)

    ls_3 = LineString([(40, -20), (40, -10), (40, 0), (40, 10)])
    assert ls_1.covers(ls_3)

    ls_4 = ls_2.merge(ls_3)
    assert ls_1.covers(ls_4)


def test_polygon():
    """
    Ensure the intersecting line strings cover each other.
    """
    poly_1 = Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
    poly_2 = Polygon([(11, 0), (21, 0), (21, 10), (11, 10), (11, 0)])
    poly_3 = Polygon([(5, 0), (15, 0), (15, 10), (5, 10), (5, 0)])

    assert not poly_1.covers(poly_2)
    assert poly_3.covers(poly_2)
    assert poly_3.covers(poly_1)

    poly_4 = poly_1.merge(poly_2)
    assert poly_4.covers(poly_1)
    assert poly_4.covers(poly_2)

    lon_min, lat_min, lon_max, lat_max = poly_1.bounding_box_corners
    assert lon_min == 0
    assert lon_max == 10
    assert lat_min == 0
    assert lat_max == 10

def test_multi_polygon():
    """
    Ensure the intersecting line strings cover each other.
    """
    poly_1 = MultiPolygon(
        [
            [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
            [(11, 0), (21, 0), (21, 10), (11, 10), (11, 0)],
        ]
    )
    poly_2 = Polygon([(5, 0), (15, 0), (15, 10), (5, 10), (5, 0)])

    assert poly_1.covers(poly_2)

    lon_min, lat_min, lon_max, lat_max = poly_1.bounding_box_corners
    assert lon_min == 0
    assert lon_max == 21
    assert lat_min == 0
    assert lat_max == 10

def test_save_and_load(tmp_path):
    """
    Ensure the saving and loading of Polygons works.
    """
    poly = Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
    poly.save(tmp_path / "poly.json")
    loaded = Polygon.load(tmp_path / "poly.json")

    assert poly.geometry == loaded.geometry


def test_lonlats_to_polygon():
    """
    Ensure that parsing of coverage polygon from lon and lat fields works.
    """
    poly_1 = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    lats = np.linspace(0, 10, 100)
    lons = np.linspace(0, 10, 100)
    lons, lats = np.meshgrid(lons, lats)
    poly_2 = lonlats_to_polygon(lons, lats)

    assert poly_1.geometry == poly_2.geometry
