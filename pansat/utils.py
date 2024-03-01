"""
pansat.utils
============

Miscellaneous utility functions.
"""
from math import ceil
import numpy as np
from pyresample import AreaDefinition, SwathDefinition
from pyresample import kd_tree
import xarray as xr

from pansat import geometry


def parse_polygon(
    lons: np.ndarray, lats: np.ndarray, n_points: int = 2
) -> geometry.Polygon:
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
        A pansat.geometry.Geometry object outlining the spatial coverage
        represented by the two coordinate arrays.
    """
    lons = lons.copy()
    lats = lats.copy()
    lons[lons == np.inf] = np.nan
    lons[lons == -np.inf] = np.nan
    lats[lats == np.inf] = np.nan
    lats[lats == -np.inf] = np.nan

    dataset = xr.Dataset({"lons": (("y", "x"), lons), "lats": (("y", "x"), lats)})
    dataset = dataset.ffill("x").ffill("y")
    dataset = dataset.bfill("x").ffill("y")

    lons = dataset.lons.data
    lats = dataset.lats.data

    lon_coords = []
    lat_coords = []

    inds = np.linspace(0, lons.shape[1] - 1, n_points).astype(np.int64)
    lon_coords += list(lons[0, inds])
    lat_coords += list(lats[0, inds])

    inds = np.linspace(0, lons.shape[0] - 1, n_points).astype(np.int64)
    lon_coords += list(lons[inds, -1])
    lat_coords += list(lats[inds, -1])

    inds = np.linspace(lons.shape[1] - 1, 0, n_points).astype(np.int64)
    lon_coords += list(lons[-1, inds])
    lat_coords += list(lats[-1, inds])

    inds = np.linspace(lons.shape[0] - 1, 0, n_points).astype(np.int64)
    lon_coords += list(lons[inds, 0])
    lat_coords += list(lats[inds, 0])

    coords = list(zip(lon_coords, lat_coords))

    polygon = geometry.Polygon(coords)
    return polygon


def get_equal_area_grid(
    longitude: float, latitude: float, resolution: float = 4e3, extent: float = 400e3
) -> AreaDefinition:
    """
    Get equal area grid center on a point.

    Args:
        longitude: The longitude coordinate of the point.
        latitude: The latitude coordinate of the point.
        resolution: The resolution of the grid.
        extent: The extent of the full grid.

    Return:
        A AreaDefinition object defining an equal area grid center on the
        given point.
    """
    width = ceil(extent / resolution)
    return AreaDefinition(
        "collocation_grid",
        "Equal-area grid for collocation",
        "colocation_grid",
        projection={
            "proj": "laea",
            "lon_0": longitude,
            "lat_0": latitude,
            "units": "m",
        },
        width=width,
        height=width,
        area_extent=(-extent / 2.0, -extent / 2.0, extent / 2.0, extent / 2.0),
    )


def resample_data(
    dataset, target_grid, radius_of_influence=5e3, new_dims=("latitude", "longitude")
) -> xr.Dataset:
    """
    Resample xarray.Dataset data to global grid.

    Args:
        dataset: xr.Dataset containing data to resample to global grid.
        target_grid: A pyresample.AreaDefinition defining the global grid
            to which to resample the data.

    Return:
        An xarray.Dataset containing the give dataset resampled to
        the global grid.
    """
    lons = dataset.longitude.data
    lats = dataset.latitude.data

    if lats.ndim == 1:
        dataset = dataset.transpose(..., "latitude", "longitude")
        lons, lats = np.meshgrid(lons, lats)

    if isinstance(target_grid, tuple):
        lons_t, lats_t = target_grid
        shape = lons_t.shape
    else:
        lons_t, lats_t = target_grid.get_lonlats()
        shape = target_grid.shape

    valid_pixels = (
        (lons_t >= np.nanmin(lons))
        * (lons_t <= np.nanmax(lons))
        * (lats_t >= np.nanmin(lats))
        * (lats_t <= np.nanmax(lats))
    )

    swath = SwathDefinition(lons=lons, lats=lats)
    target = SwathDefinition(lons=lons_t[valid_pixels], lats=lats_t[valid_pixels])

    info = kd_tree.get_neighbour_info(
        swath, target, radius_of_influence=radius_of_influence, neighbours=1
    )
    ind_in, ind_out, inds, _ = info


    resampled = {}
    resampled["latitude"] = (("latitude",), lats_t[:, 0])
    resampled["longitude"] = (("longitude",), lons_t[0, :])

    for var in dataset:
        if var in ["latitude", "longitude"]:
            continue
        data = dataset[var].data
        if data.ndim == 1 and lons.ndim > 1:
            data = np.broadcast_to(data[:, None], lons.shape)

        dtype = data.dtype
        if np.issubdtype(dtype, np.datetime64):
            fill_value = np.datetime64("NaT")
        elif np.issubdtype(dtype, np.integer):
            fill_value = -9999
        elif dtype == np.int8:
            fill_value = -1
        else:
            fill_value = np.nan

        data_r = kd_tree.get_sample_from_neighbour_info(
            "nn", target.shape, data, ind_in, ind_out, inds, fill_value=fill_value
        )

        data_full = np.zeros(shape + data.shape[lons.ndim :], dtype=dtype)
        if np.issubdtype(dtype, np.floating):
            data_full = np.nan * data_full
        elif np.issubdtype(dtype, np.datetime64):
            data_full[:] = np.datetime64("NaT")
        elif dtype == np.int8:
            data_full[:] = -1
        else:
            data_full[:] = -9999

        data_full[valid_pixels] = data_r
        resampled[var] = (new_dims + dataset[var].dims[lons.ndim :], data_full)

    return xr.Dataset(resampled)
