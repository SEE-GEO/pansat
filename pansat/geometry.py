"""
pansat.geometry
===============

The pansat geometry provides functions to represent the spatial coverage
of data files using geometrical objects.
"""
import numpy as np
from shapely.geometry import Polygon, MultiPolygon
from shapely.validation import make_valid
from shapely.ops import unary_union


def parse_point(xml_point):
    """
    Parse point from XML file.
    """
    lon = float(xml_point[0].text)
    lat = float(xml_point[1].text)
    return [lon, lat]


def parse_polygon_xml(xml_polygon):
    """
    Parse polygon from XML file.
    """
    boundary = xml_polygon[0]
    points = np.array(list(map(parse_point, boundary)))
    dlons = points[:, 0] - points[0, 0]
    indices = np.where(np.abs(dlons) > 180)[0]
    for ind in indices:
        if points[ind, 0] > 0:
            points[ind, 0] -= 360
        else:
            points[ind, 0] += 360

    return make_valid(Polygon(points))


def parse_swath_xml(meta_data):
    """
    Parse shapes describing a satellite swath from metadata.

    Args:
        meta_data: XML tree containing the meta data.

    Return:
        List of polygons.
    """
    sdc = meta_data.find("SpatialDomainContainer")
    hsdc = sdc.find("HorizontalSpatialDomainContainer")

    polygons = list(map(parse_polygon_xml, hsdc))
    new_polygons = []
    poles = []

    for ind in range(len(polygons)):
        poly = polygons[ind]
        points = np.array(poly.convex_hull.exterior.coords)
        if any(points[:, 1] > 85):
            pole = Polygon([[-180, 70], [180, 70], [180, 90], [-180, 90]])
            poles.append(pole)
        elif any(points[:, 1] < -85):
            pole = Polygon([[-180, -70], [180, -70], [180, -90], [-180, -90]])
            poles.append(pole)
        else:
            new_polygons.append(poly)

    multi = unary_union(new_polygons + poles)
    return multi


def handle_poles(polygons):
    """
    Handle poles in a sequence of Polygons representing a swath from
    a polar orbiting satellite.

    The 2D representation of satellite observations breaks down along
    the poles. This function checks whether the observations extend
    outside of +/- 70 degree latitude and if so simply add a polygon
    covering all of the polar regions.

    Args:
        polygons: A list of shapely polygons.

    Return:
        The list of polygons with offending Polygons replaced.
    """
    for ind in range(len(polygons)):
        poly = polygons[ind]
        points = np.stack(poly.exterior.coords.xy, -1)
        if any(points[:, 1] > 70):
            poly_2 = Polygon([[-180, 75], [180, 75], [180, 90], [-180, 90]])
            polygons[ind] = poly_2
        if any(points[:, 1] < -70):
            poly_2 = Polygon([[-180, -75], [180, -75], [180, -90], [-180, -90]])
            polygons[ind] = poly_2

    poly = polygons[0]
    for other in polygons[1:]:
        poly = poly.union(other)
    return poly


def parse_swath(lons, lats, m=10, n=1) -> MultiPolygon:
    """
    Parse a swath as a 'shapely.Geometry'.

    Args:
        lons: A 2D array holding the longitude coordinates of the given
            observations.
        lats: A 2D array hodling the latitude coordinates of the
            observations.

    Return:
        A shapely geometry representing the observations.
    """
    n_i = lons.shape[0]
    n_j = lons.shape[1]
    d_i = n_i // m
    d_j = n_j // n
    ind_i = 0

    polys = []

    while ind_i < lons.shape[0]:
        ind_j = 0
        while ind_j < lons.shape[1]:
            lon_0_0 = lons[ind_i, ind_j]
            lat_0_0 = lats[ind_i, ind_j]
            lon_0_1 = lons[ind_i, min(ind_j + d_j, n_j - 1)]
            lat_0_1 = lats[ind_i, min(ind_j + d_j, n_j - 1)]
            lon_1_1 = lons[min(ind_i + d_i, n_i - 1), min(ind_j + d_j, n_j - 1)]
            lat_1_1 = lats[min(ind_i + d_i, n_i - 1), min(ind_j + d_j, n_j - 1)]
            lon_1_0 = lons[min(ind_i + d_i, n_i - 1), ind_j]
            lat_1_0 = lats[min(ind_i + d_i, n_i - 1), ind_j]

            polys.append(
                Polygon(
                    [
                        [lon_0_0, lat_0_0],
                        [lon_0_1, lat_0_1],
                        [lon_1_1, lat_1_1],
                        [lon_1_0, lat_1_0],
                        [lon_0_0, lat_0_0],
                    ]
                )
            )
            ind_j += d_j
        ind_i += d_i
    poly = handle_poles(polys)
    return make_valid(poly)
