"""
Tests for the pansat.utils module.
==================================
"""
import numpy as np
from pyresample import SwathDefinition
import xarray as xr

from pansat.utils import resample_data, resample_unique


def test_resampling_1d():
    """
    Test resample for one-to-several and one-to-one scenarios for 1D data.
    """
    source = xr.Dataset({
        "longitude": (("samples",), np.array([-0.1, 0.1, 10.0])),
        "latitude": (("samples",), np.array([-0.1, 0.1, 10.0])),
        "precip_source": (("samples",), np.zeros(3)),
        "precip_profiles": (("samples", "levels"), np.zeros((3, 11)))
    })
    target = xr.Dataset({
        "longitude": (("samples",), np.array([-0.5, -0.5, 0.5, 0.5])),
        "latitude": (("samples",), np.array([-0.5, 0.5, 0.5, -0.5])),
    })

    source_swath = SwathDefinition(lons=source.longitude.data, lats=source.latitude.data)
    target_swath = SwathDefinition(lons=target.longitude.data, lats=target.latitude.data)

    source_r = resample_data(source, target_swath, radius_of_influence=200e3, new_dims=("samples",))
    assert np.all(np.isclose(source_r.precip_source.data, 0.0))
    assert source_r.precip_profiles.ndim == 2

    source_r = resample_data(source, target_swath, radius_of_influence=200e3, new_dims=("samples",), unique=True)
    assert np.isfinite(source_r.precip_source.data).sum() == 2
    assert source_r.precip_profiles.ndim == 2


def test_resampling_1d_to_2d():
    """
    Test resampling of 1D data to 2D data.
    """
    source = xr.Dataset({
        "longitude": (("samples",), np.array([-0.1, 0.1, 10.0])),
        "latitude": (("samples",), np.array([-0.1, 0.1, 10.0])),
        "precip_source": (("samples",), np.ones(3)),
        "precip_profile": (("samples", "levels"), np.ones((3, 11)))
    })
    target = xr.Dataset({
        "longitude": (("longitude",), np.array([-0.5, 0.5])),
        "latitude": (("latitude",), np.array([-0.5, 0.5])),
    })

    source_swath = SwathDefinition(lons=source.longitude.data, lats=source.latitude.data)
    lons, lats = np.meshgrid(target.longitude.data, target.latitude.data)
    target_swath = SwathDefinition(lons=lons, lats=lats)

    source_r = resample_data(source, target_swath, radius_of_influence=200e3)
    assert np.all(np.isclose(source_r.precip_source.data, 1.0))
    assert source_r.precip_profile.ndim == 3

    source_r = resample_data(source, target_swath, radius_of_influence=200e3, unique=True)
    assert np.isfinite(source_r.precip_source.data).sum() == 2
    assert source_r.precip_profile.ndim == 3


def test_resampling_2d_to_2d():
    """
    Test resampling of 2D data from regular and irregular grids.
    """
    # Regular grid
    source = xr.Dataset({
        "longitude": (("longitude",), np.array([-0.1, 0.1])),
        "latitude": (("latitude",), np.array([-0.1, 0.1])),
        "precip_source": (("latitude", "longitude",), np.ones((2, 2))),
        "precip_profile": (("latitude", "longitude", "levels"), np.ones((2, 2, 11)))
    })
    target = xr.Dataset({
        "longitude": (("longitude",), np.array([-0.5, 0.5])),
        "latitude": (("latitude",), np.array([-0.5, 0.5])),
    })

    source_swath = SwathDefinition(lons=source.longitude.data, lats=source.latitude.data)
    lons, lats = np.meshgrid(target.longitude.data, target.latitude.data)
    target_swath = SwathDefinition(lons=lons, lats=lats)

    source_r = resample_data(source, target_swath, radius_of_influence=200e3)
    assert np.all(np.isclose(source_r.precip_source.data, 1.0))
    assert source_r.precip_profile.ndim == 3

    source_r = resample_data(source, target_swath, radius_of_influence=200e3, unique=True)
    assert np.isfinite(source_r.precip_source.data).sum() == 4
    assert source_r.precip_profile.ndim == 3


    # 2D swath
    source = xr.Dataset({
        "longitude": (("scans", "pixels",), np.array([[-0.1, 0.1], [-0.1, 0.1]])),
        "latitude": (("scans", "pixels",), np.array([[-0.1, -0.1], [0.1, 0.1]])),
        "precip_source": (("scans", "pixels",), np.ones((2, 2))),
        "precip_profile": (("scans", "pixels", "levels"), np.ones((2, 2, 11)))
    })
    target = xr.Dataset({
        "longitude": (("longitude",), np.array([-0.5, 0.5])),
        "latitude": (("latitude",), np.array([-0.5, 0.5])),
    })

    source_swath = SwathDefinition(lons=source.longitude.data, lats=source.latitude.data)
    lons, lats = np.meshgrid(target.longitude.data, target.latitude.data)
    target_swath = SwathDefinition(lons=lons, lats=lats)

    source_r = resample_data(source, target_swath, radius_of_influence=200e3)
    assert np.all(np.isclose(source_r.precip_source.data, 1.0))
    assert source_r.precip_profile.ndim == 3

    source_r = resample_data(source, target_swath, radius_of_influence=200e3, unique=True)
    assert np.isfinite(source_r.precip_source.data).sum() == 4
    assert source_r.precip_profile.ndim == 3
