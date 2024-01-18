"""
pansat.geometry
===============

The pansat geometry provides functions to represent the spatial coverage
of data files using geometrical objects.
"""
from abc import ABC, abstractmethod
import json
from pathlib import Path

import numpy as np
import shapely
from shapely.geometry import Polygon, MultiPolygon
from shapely.validation import make_valid
from shapely.ops import unary_union
from shapely.validation import make_valid
import xarray as xr


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
        if any(points[:, 1] > 75):
            poly_2 = Polygon([[-180, 75], [180, 75], [180, 90], [-180, 90]])
            polygons[ind] = poly_2
        if any(points[:, 1] < -75):
            poly_2 = Polygon([[-180, -75], [180, -75], [180, -90], [-180, -90]])
            polygons[ind] = poly_2

    if len(polygons) == 0:
        return Polygon()

    poly = make_valid(polygons[0])
    for other in polygons[1:]:
        poly = poly.union(make_valid(other))
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

            d_lon = max(
                [
                    abs(lon_0_0 - lon_0_1),
                    abs(lon_0_1 - lon_1_1),
                    abs(lon_1_1 - lon_1_0),
                    abs(lon_1_0 - lon_0_0),
                ]
            )
            if d_lon < 240:
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
            else:
                polys.append(
                    Polygon(
                        [
                            [lon_0_0 % 360, lat_0_0],
                            [lon_0_1 % 360, lat_0_1],
                            [lon_1_1 % 360, lat_1_1],
                            [lon_1_0 % 360, lat_1_0],
                            [lon_0_0 % 360, lat_0_0],
                        ]
                    )
                )
                polys.append(
                    Polygon(
                        [
                            [lon_0_0 % 360 - 360, lat_0_0],
                            [lon_0_1 % 360 - 360, lat_0_1],
                            [lon_1_1 % 360 - 360, lat_1_1],
                            [lon_1_0 % 360 - 360, lat_1_0],
                            [lon_0_0 % 360 - 360, lat_0_0],
                        ]
                    )
                )

            ind_j += d_j
        ind_i += d_i
    poly = handle_poles(polys)
    return make_valid(poly)


###############################################################################
# Geometries classes
###############################################################################


class Geometry(ABC):
    """
    Generic interface for objects representation the spatial coverage
    of product files.

    Although most geometry classes just wrap around a shapely object,
    the purpose of the ABC is to serve as an abstraction layer to simplify
    potential switching to a geometry backend that is more adept for
    handling spherical geometries.
    """

    @abstractmethod
    def covers(self, other: "Geometry") -> bool:
        """
        Predicate function indicating wheterh the geometry covers (or
        intersects) another geometry.
        """
        pass

    @abstractmethod
    def to_shapely(self):
        """
        Convert geometry to a shapely geometry.
        """
        pass

    def to_geojson(self):
        return shapely.to_geojson(self.to_shapely())

    def merge(self, other):
        """
        Merge a geometry with another.
        """

    @property
    def bounding_box_corners(self):
        """
        Calculate corners of bounding box of the geometry.

        Rerturn:
            A tuple (lon_min, lat_min, lon_max, lat_min) containing the
            longitude and latitude coordinates of the lower left corner
            of the bounding box ('lon_min' and 'lat_min') as well as
            the upper right corner ('lon_max' and 'lat_max').
        """
        shapely_geo = self.to_shapely().convex_hull
        if hasattr(shapely_geo, "exterior"):
            coords = np.array(shapely_geo.exterior.coords)
        else:
            coords = np.array(shapely_geo.coords)
        lons = coords[:, 0]
        lats = coords[:, 1]
        lon_min = np.min(lons)
        lon_max = np.max(lons)
        lat_min = np.min(lats)
        lat_max = np.max(lats)
        if lon_max - lon_min > 180:
            return [
                (-180, lat_min, lon_min, lat_max),
                (lon_max, lat_min, lon_max, lat_max)
            ]
        return (lon_min, lat_min, lon_max, lat_max)


    def _repr_html_(self):
        try:
            from ipyleaflet import Map, WKTLayer
            from IPython.display import display

            llmap = Map(zoom=1)
            layer = WKTLayer(wkt_string=self.to_shapely().wkt)
            llmap.add_layer(layer)
            display(llmap)
        except ModuleNotFoundError:
            return NotImplemented


class ShapelyGeometry(Geometry):
    """
    A geometry class that internally uses a shapely.Geometry
    """

    def __init__(self, geometry):
        """
        Args: A shapely geometry object.
        """
        self.geometry = geometry

    def covers(self, other: Geometry) -> bool:
        return self.geometry.intersects(other.to_shapely())

    def to_shapely(self):
        return self.geometry

    def merge(self, other):
        geometry = self.geometry.union(other.to_shapely())
        return ShapelyGeometry(geometry)

    @classmethod
    def from_geojson(cls, dct):
        """
        Parse object from geojson.
        """
        return cls(shapely.from_geojson(dct))

    @classmethod
    def load(cls, path: Path):
        """
        Load geometry object from geojson file.

        Args:
            path: A path object pointing to the file to load.

        Return:
            The loaded shapely geometry object.
        """
        with open(path, "r") as input:
            return cls.from_geojson(json.load(input))

    def save(self, path: Path):
        """
        Save geometry object as geojson file.

        Args:
            path: A path object pointing to the file to which to write
                this geometry.
        """
        with open(path, "w") as output:
            json.dump(shapely.to_geojson(self.geometry), output)


class LonLatRect(ShapelyGeometry):
    """
    A rectangular domain in latitude and longitude coordinates.
    """

    def __init__(self, lon_min, lat_min, lon_max, lat_max):
        """
        Args:
            lon_min: Longitude coord. of the lower left corner of the domain.
            lat_min: Latitude coord. of the lower left corner of the domain.
            lon_max: Longitude coord. of the upper right corner of the domain.
            lat_max: Latitude coord. of the upper right corner of the domain.
        """
        self.lon_min = lon_min
        self.lat_min = lat_min
        self.lon_max = lon_max
        self.lat_max = lat_max
        super().__init__(shapely.box(lon_min, lat_min, lon_max, lat_max))

    def __repr__(self):
        return (
            f"LonLatRect(lon_min={self.lon_min}, lat_min={self.lat_min}, "
            f"lon_max={self.lon_max}, lat_max={self.lat_max})"
        )


class LineString(ShapelyGeometry):
    """
    A string of points connected by a line.
    """

    def __init__(self, coords):
        super().__init__(shapely.LineString(coords))


class MultiLineString(ShapelyGeometry):
    """
    A combination of multiple line strings.
    """

    def __init__(self, coords):
        super().__init__(shapely.MultiLineString(coords))


class Polygon(ShapelyGeometry):
    """
    A polygon geometry that internally uses a shapely polygon.
    """

    def __init__(self, poly):
        if not isinstance(poly, shapely.Polygon):
            poly = shapely.validation.make_valid(shapely.Polygon(poly))
        super().__init__(poly)


class MultiPolygon(ShapelyGeometry):
    """
    A multi-polygon geometry that internally uses a shapely
    multi-polygon.
    """

    def __init__(self, polys):
        if not isinstance(polys, shapely.MultiPolygon):
            polys = shapely.validation.make_valid(
                shapely.MultiPolygon([shapely.Polygon(poly) for poly in polys])
            )
        super().__init__(polys)


class SatelliteSwath(ShapelyGeometry):
    """
    A satellite swath represented using a shapely geometry.
    """

    def __init__(self, geometry):
        super().__init__(geometry)


def calc_intersect_antimeridian(p_1, p_2):
    """
    Calculate latitude coordinate of a line intersecting the antimeridian.

    Args:
        p_1: Starting point of the line.
        p_2: End point of the line.

    Return:
        The latitude coordinate of the intersection with the antimerdian.
    """
    if p_1[0] > p_2[0]:
        p_1, p_2 = p_2, p_1

    lon_1 = p_1[0]
    lon_2 = p_2[0]
    delta_lon = 360 - (lon_2 - lon_1)

    lat_1 = p_1[1]
    lat_2 = p_2[1]
    delta_lat = lat_1 - lat_2

    return lat_2 + delta_lat * (180 - lon_2) / delta_lon


def split_at_antimeridian(coords):
    """
    Split a polygon reprsented by tuples of coords at antimeridian.

    Args:
        coords: A list of tuples containing each longitude and latitude
            coordinates of the points in the polygon.

    Return:
        A list the parts of the polygon so that none of the crosses
        the antimeridian.
    """
    parts = {0: []}
    loc = 0

    curr_point = coords[0]
    for next_point in coords[1:] + [curr_point]:
        parts[loc].append(curr_point)

        if abs(curr_point[0] - next_point[0]) > 180:
            lat_new = calc_intersect_antimeridian(curr_point, next_point)
            # Leaving to the left
            if curr_point[0] < next_point[0]:
                parts[loc].append((-180, lat_new))
                loc -= 1
                parts.setdefault(loc, []).append((180, lat_new))
            # Leaving to the right
            else:
                parts[loc].append((180, lat_new))
                loc += 1
                parts.setdefault(loc, []).append((-180, lat_new))

        curr_point = next_point

    return list(parts.values())


def get_first_valid(data: np.ndarray):
    """
    Get first valid element in an array.
    """
    ind = np.where(np.isfinite(data))[0][0]
    return data[ind]


def get_last_valid(data: np.ndarray):
    """
    Get last valid element in an array.
    """
    ind = np.where(np.isfinite(data))[0][-1]
    return data[ind]


def lonlats_to_polygon(
    lons: np.ndarray, lats: np.ndarray, n_points: int = 2
) -> Polygon:
    """
    Parse polygon from 2D longitude and latitude fields.

    This function assumes that lons and lats reprsent a continuous
    field of coordinates.

    Args:
        lons: A 2D numpy array containing longitude coordinates
        lats: A 2D numpy array containing latitude coordinates.
        n_points: The number of points to use along each edge of
            the array.

    Return:
        A Polygon object outlining the spatial coverage
        represented by the two coordinate arrays.
    """

    lon_coords = []
    lat_coords = []

    mask = np.isfinite(lons) * np.isfinite(lats)

    if not np.all(mask):
        valid = np.where(np.mean(mask, 0) > np.sqrt(0.5))[0]
        inds = np.linspace(0, len(valid) - 1, n_points).astype(np.int64)
        lon_coords += [get_first_valid(lons[:, ind]) for ind in valid[inds]]
        lat_coords += [get_first_valid(lats[:, ind]) for ind in valid[inds]]

        valid = np.where(np.mean(mask, 1) > np.sqrt(0.5))[0]
        inds = np.linspace(0, len(valid) - 1, n_points).astype(np.int64)
        lon_coords += [get_last_valid(lons[ind, :]) for ind in valid[inds[1:]]]
        lat_coords += [get_last_valid(lats[ind, :]) for ind in valid[inds[1:]]]

        valid = np.where(np.mean(mask, 0) > np.sqrt(0.5))[0]
        inds = np.linspace(len(valid) - 1, 0, n_points).astype(np.int64)
        lon_coords += [get_last_valid(lons[:, ind]) for ind in valid[inds[1:]]]
        lat_coords += [get_last_valid(lats[:, ind]) for ind in valid[inds[1:]]]

        valid = np.where(np.mean(mask, 1) > np.sqrt(0.5))[0]
        inds = np.linspace(len(valid) - 1, 0, n_points).astype(np.int64)
        lon_coords += [get_first_valid(lons[ind, :]) for ind in valid[inds[1:-1]]]
        lat_coords += [get_first_valid(lats[ind, :]) for ind in valid[inds[1:-1]]]
    else:
        inds = np.linspace(0, lons.shape[1] - 1, n_points).astype(np.int64)
        lon_coords += [lons[0, ind] for ind in inds]
        lat_coords += [lats[0, ind] for ind in inds]

        inds = np.linspace(0, lons.shape[0] - 1, n_points).astype(np.int64)
        lon_coords += [lons[ind, -1] for ind in inds[1:]]
        lat_coords += [lats[ind, -1] for ind in inds[1:]]

        inds = np.linspace(lons.shape[1] - 1, 0, n_points).astype(np.int64)
        lon_coords += [lons[-1, ind] for ind in inds[1:]]
        lat_coords += [lats[-1, ind] for ind in inds[1:]]

        inds = np.linspace(lons.shape[0] - 1, 0, n_points).astype(np.int64)
        lon_coords += [lons[ind, 0] for ind in inds[1:-1]]
        lat_coords += [lats[ind, 0] for ind in inds[1:-1]]

    coords = list(zip(lon_coords, lat_coords))
    coords = split_at_antimeridian(coords)

    if len(coords) == 1:
        print(coords[0])
        return Polygon(coords[0])

    mpolygon = MultiPolygon(coords)
    return mpolygon
