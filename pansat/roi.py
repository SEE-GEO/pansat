"""
pansat.roi
==========

Functions to define and work with regions of interest (ROIs).
"""
import numpy as np


class ROI:
    def __init__(self, lon_ll, lat_ll, lon_ur, lat_ur):
        self._components = np.array([lon_ll, lat_ll, lon_ur, lat_ur])

    @property
    def lon_min(self):
        return self._components[0]

    @property
    def lon_ll(self):
        return self._components[0]

    @property
    def lat_min(self):
        return self._components[1]

    @property
    def lat_ll(self):
        return self._components[1]

    @property
    def lon_max(self):
        return self._components[2]

    @property
    def lon_ur(self):
        return self._components[2]

    @property
    def lat_max(self):
        return self._components[3]

    @property
    def lat_ur(self):
        return self._components[3]

    def __iter__(self):
        yield from self._components

    def __repr__(self):
        return f"ROI({self.lon_ll}, {self.lat_ll}, {self.lon_ur}, {self.lat_ur})"

    def __getitem__(self, i):
        if (i < 0) or (i < 0):
            raise ValueError("The bounding box has only for elements.")
        return self._components[i]

    def __setitem__(self, i, val):
        if (i < 0) or (i < 0):
            raise ValueError("The bounding box has only for elements.")
        self._components[i] = val

    def to_geometry(self):
        """
        Return representation of ROI as ``shapely.geometry.Polygon``.
        """
        from shapely.geometry import Polygon

        return Polygon(
            [
                [self.lon_min, self.lat_min],
                [self.lon_max, self.lat_min],
                [self.lon_max, self.lat_max],
                [self.lon_min, self.lat_max],
            ]
        )


class PolygonROI:
    def __init__(self, points):
        self.points = points

    @property
    def lon_min(self):
        return self.points[:, 0].min()

    @property
    def lon_ll(self):
        return self.points[:, 0].min()

    @property
    def lat_min(self):
        return self.points[:, 1].min()

    @property
    def lat_ll(self):
        return self.points[:, 1].min()

    @property
    def lon_max(self):
        return self.points[:, 0].max()

    @property
    def lon_ur(self):
        return self.points[:, 0].max()

    @property
    def lat_max(self):
        return self.points[:, 1].max()

    @property
    def lat_ur(self):
        return self.points[:, 1].max()

    def __iter__(self):
        yield from self.points

    def __repr__(self):
        return f"PolyROI({self.points})"

    def to_geometry(self):
        """
        Return representation of ROI as ``shapely.geometry.Polygon``.
        """
        from shapely.geometry import Polygon

        return Polygon(self.points)


def _get_lats_and_lons(data):
    """
    Get latitude and longitude coordinates from xarray dataset.

    Searches for variables named 'lat' or 'latitude' and 'lon' and
    'longitude' and returns them as a pair.

    Args:
        data: An xarray dataset with satellite swath data.

    Return:
        A tuple ``(lat, lon)`` containing the latitude and longitude
        arrays of the dataset.
    """
    if "latitude" in data:
        lats = data.latitude
    elif "lat" in data:
        lats = data.lat
    else:
        raise ValueError("Neither 'lat' nor 'latitude' variable is present in dataset.")

    if "longitude" in data:
        lons = data.longitude
    elif "lon" in data:
        lons = data.lon
    else:
        raise ValueError(
            "Neither 'lon' nor 'longitude' variable is present in dataset."
        )

    return lats, lons


def find_overpasses(roi, data):
    lats, lons = _get_lats_and_lons(data)
    lats = lats.data
    lons = lons.data

    axes = tuple(range(lats.ndim))[1:]
    indices = np.where(
        np.any(
            (lats >= roi.lat_min)
            * (lats <= roi.lat_max)
            * (lons >= roi.lon_min)
            * (lons <= roi.lon_max),
            axis=axes,
        )
    )[0]

    if len(indices) == 0:
        return []

    changes = np.diff(indices)
    gaps = np.where(changes > 1)[0]

    first_dim = next(iter(data.dims.keys()))
    i_start = indices[0]

    slices = []

    offset = 0
    for g in gaps:
        i_start = indices[0]
        i_end = indices[g - offset]
        indices = indices[g - offset + 1 :]
        offset = g + 1
        slices.append(data[{first_dim: slice(i_start, i_end + 1)}])

    i_start = indices[0]
    i_end = indices[-1]
    slices.append(data[{first_dim: slice(i_start, i_end + 1)}])
    return slices


def any_inside(roi, data):
    lats, lons = _get_lats_and_lons(data)
    lats = lats.data
    lons = lons.data

    axes = tuple(range(lats.ndim))[1:]
    return np.any(
        np.any(
            (lats >= roi.lat_min)
            * (lats <= roi.lat_max)
            * (lons >= roi.lon_min)
            * (lons <= roi.lon_max),
            axis=axes,
        )
    )


def some_inside(roi, data, fraction=0.5):
    lats, lons = _get_lats_and_lons(data)
    lats = lats.data
    lons = lons.data

    axes = tuple(range(lats.ndim))[1:]
    inside = np.mean(
        (lats >= roi.lat_min)
        * (lats <= roi.lat_max)
        * (lons >= roi.lon_min)
        * (lons <= roi.lon_max),
        axis=axes,
    )
    return np.any(inside > fraction)
