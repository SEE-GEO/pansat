"""
pansat.utils
============

Miscellaneous utility functions.
"""
from math import ceil
from typing import Tuple

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
        dataset: xr.Dataset,
        target_grid: AreaDefinition,
        radius_of_influence: float = 5e3,
        new_dims: Tuple[str, str] = ("latitude", "longitude"),
        unique: bool = False
) -> xr.Dataset:
    """
    Resample xarray.Dataset data to a given target grid.

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

    if ("latitude" in dataset.dims) and ("longitude" in dataset.dims):
        dataset = dataset.transpose("latitude", "longitude", ...)
        lons, lats = np.meshgrid(lons, lats)
    else:
        spatial_dims = dataset.latitude.dims
        dataset = dataset.transpose(*spatial_dims, ...)

    if isinstance(target_grid, tuple):
        lons_t, lats_t = target_grid
        shape = lons_t.shape
    else:
        lons_t, lats_t = target_grid.get_lonlats()
        shape = target_grid.shape

    lon_min = np.nanmin(lons) - radius_of_influence / 100e3
    lon_max = np.nanmax(lons) + radius_of_influence / 100e3
    lat_min = np.nanmin(lats) - radius_of_influence / 100e3
    lat_max = np.nanmax(lats) + radius_of_influence / 100e3
    valid_pixels_target = (
        (lon_min <= lons_t)
        * (lons_t <= lon_max)
        * (lat_min <= lats_t)
        * (lats_t <= lat_max)
    )

    lon_min_t = np.nanmin(lons_t) - radius_of_influence / 100e3
    lon_max_t = np.nanmax(lons_t) + radius_of_influence / 100e3
    lat_min_t = np.nanmin(lats_t) - radius_of_influence / 100e3
    lat_max_t = np.nanmax(lats_t) + radius_of_influence / 100e3
    valid_pixels_source = (
        (lon_min_t <= lons)
        * (lons <= lon_max_t)
        * (lat_min_t <= lats)
        * (lats <= lat_max_t)
    )
    n_valid_source = valid_pixels_source.sum()

    swath = SwathDefinition(lons=lons[valid_pixels_source], lats=lats[valid_pixels_source])
    target = SwathDefinition(lons=lons_t[valid_pixels_target], lats=lats_t[valid_pixels_target])

    if unique:
        info = kd_tree.get_neighbour_info(
            target, swath, radius_of_influence=radius_of_influence, neighbours=1
        )
        ind_out, ind_in, inds, _ = info
    else:
        info = kd_tree.get_neighbour_info(
            swath, target, radius_of_influence=radius_of_influence, neighbours=1
        )
        ind_in, ind_out, inds, _ = info


    resampled = {}

    if lats_t.ndim > 1 and np.isclose(lats_t[:, 0], lats_t[:, 1]).all():
        resampled["latitude"] = (new_dims[0], lats_t[:, 0])
        resampled["longitude"] = (new_dims[1], lons_t[0, :])
    else:
        resampled["latitude"] = (new_dims, lats_t)
        resampled["longitude"] = (new_dims, lons_t)


    for var in dataset:
        if var in ["latitude", "longitude"]:
            continue
        data = dataset[var].data

        if data.ndim == 0:
            resampled[var] = data
            continue

        if data.ndim == 1 and lons.ndim > 1:
            data = np.broadcast_to(data[:, None], lons.shape)

        data = data[valid_pixels_source]

        dtype = data.dtype
        if np.issubdtype(dtype, np.datetime64):
            fill_value = np.datetime64("NaT")
        elif dtype == np.int8:
            fill_value = -1
        elif np.issubdtype(dtype, np.integer):
            fill_value = -9999
        else:
            fill_value = np.nan

        target_shape = target.shape
        #if data.ndim > lons.ndim:
        #    target_shape = target_shape + data.shape[lons.ndim:]

        shape_orig = data.shape[1:]
        if data.ndim > 1:
            data_flat = data.reshape(n_valid_source, -1)
        else:
            data_flat = data

        if unique:
            data_r = np.zeros((valid_pixels_target.sum(),) + shape_orig, dtype=data.dtype)
            data_r[:] = fill_value
            if data.ndim > 1:
                data_r[inds] = np.where(ind_in[..., None], data_flat, fill_value)
            else:
                data_r[inds] = np.where(ind_in, data_flat, fill_value)
        else:
            data_r = kd_tree.get_sample_from_neighbour_info(
                "nn", target_shape, data_flat, ind_in, ind_out, inds, fill_value=fill_value
            )
        data_r = data_r.reshape((-1,) + shape_orig)

        data_full = np.zeros(shape + data.shape[1:], dtype=dtype)
        if np.issubdtype(dtype, np.floating):
            data_full = np.nan * data_full
        elif np.issubdtype(dtype, np.datetime64):
            data_full[:] = np.datetime64("NaT")
        elif dtype == np.int8:
            data_full[:] = -1
        else:
            data_full[:] = -9999

        data_full[valid_pixels_target] = data_r
        resampled[var] = (new_dims + dataset[var].dims[valid_pixels_source.ndim:], data_full)


    return xr.Dataset(resampled)


def resample_unique(
        dataset,
        target_grid,
        maximum_distance: float = 5e3,
        new_dims: Tuple[str, str] = ("latitude", "longitude"),
        unique: bool = False
) -> xr.Dataset:
    """
    Resample dataset to target grid but match each input sample to at most one grid
    position.

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

    if ("latitude" in dataset.dims) and ("longitude" in dataset.dims):
        dataset = dataset.transpose("latitude", "longitude", ...)
        lons, lats = np.meshgrid(lons, lats)
    else:
        spatial_dims = dataset.latitude.dims
        dataset = dataset.transpose(*spatial_dims, ...)


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
        target, swath, radius_of_influence=maximum_distance, neighbours=1
    )
    ind_out, ind_in, inds, _ = info


    resampled = {}

    if np.isclose(lats_t[:, 0], lats_t[:, 1]).all():
        resampled["latitude"] = (new_dims[0], lats_t[:, 0])
        resampled["longitude"] = (new_dims[1], lons_t[0, :])
    else:
        resampled["latitude"] = (new_dims, lats_t)
        resampled["longitude"] = (new_dims, lons_t)


    for var in dataset:
        if var in ["latitude", "longitude"]:
            continue
        data = dataset[var].data

        if data.ndim == 0:
            resampled[var] = data
            continue

        if data.ndim == 1 and lons.ndim > 1:
            data = np.broadcast_to(data[:, None], lons.shape)

        dtype = data.dtype
        if np.issubdtype(dtype, np.datetime64):
            fill_value = np.datetime64("NaT")
        elif dtype == np.int8:
            fill_value = -1
        elif np.issubdtype(dtype, np.integer):
            fill_value = -9999
        else:
            fill_value = np.nan

        target_shape = target.shape
        #if data.ndim > lons.ndim:
        #    target_shape = target_shape + data.shape[lons.ndim:]

        shape_orig = data.shape[lons.ndim:]
        data_flat = data.reshape(lons.shape +  (-1,))
        data_r = kd_tree.get_sample_from_neighbour_info(
            "nn", target_shape, data_flat, ind_in, ind_out, inds, fill_value=fill_value
        )
        data_r = data_r.reshape((-1,) + shape_orig)

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
