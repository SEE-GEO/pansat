from pansat.geometry import LineString, Polygon, MultiPolygon


def test_line_string():
    """
    Ensure the intersecting line strings cover each other.
    """
    ls_1 = LineString([(0, 0), (10, 0), (20, 0), (30, 0)])
    ls_2 = LineString([(20, -20), (20, -10), (20, 0), (20, 10)])

    assert ls_1.covers(ls_2)


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


def test_multi_polygon():
    """
    Ensure the intersecting line strings cover each other.
    """
    poly_1 = MultiPolygon([
        [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
        [(11, 0), (21, 0), (21, 10), (11, 10), (11, 0)]
    ])
    poly_2 = Polygon([(5, 0), (15, 0), (15, 10), (5, 10), (5, 0)])

    assert poly_1.covers(poly_2)
