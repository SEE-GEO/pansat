import numpy as np
from shapely.geometry import Polygon, MultiPolygon
from shapely.validation import make_valid
from shapely import unary_union


def parse_point(xml_point):
    """
    Parse point from XML file.
    """
    lon = float(xml_point[0].text)
    lat = float(xml_point[1].text)
    return [lon, lat]


def parse_polygon(xml_polygon):
    """
    Parse polygon from XML file.
    """
    boundary = xml_polygon[0]
    points = np.array(list(map(parse_point, boundary.getchildren())))
    dlons = points[:, 0] - points[0, 0]
    indices = np.where(np.abs(dlons) > 180)[0]
    for ind in indices:
        if points[ind, 0] > 0:
            points[ind, 0] -= 360
        else:
            points[ind, 0] += 360

    return make_valid(Polygon(points))


def parse_swath(meta_data):
    """
    Parse shapes describing a satellite swath from metadata.

    Args:
        meta_data: XML tree containing the meta data.

    Return:
        List of polygons.
    """
    sdc = meta_data.find("SpatialDomainContainer")
    hsdc = sdc.find("HorizontalSpatialDomainContainer")

    polygons = list(map(parse_polygon, hsdc.getchildren()))
    for ind in range(len(polygons)):
        poly = polygons[ind]
        points = np.array(poly.convex_hull.exterior.coords)
        if any(points[:, 1] > 70):
            poly_2 = Polygon([[-180, 75], [180, 75], [180, 90], [-180, 90]])
            polygons[ind] = poly.union(poly_2)
        if any(points[:, 1] < -70):
            poly_2 = Polygon([[-180, -75], [180, -75], [180, -90], [-180, -90]])
            polygons[ind] = poly.union(poly_2)

    return unary_union(polygons)


def reshape_polygon(multi_polygon):
    to_centroid = lambda x: np.array(x.centroid())
    centroids = map()
    polys = multi_polygon.geoms
