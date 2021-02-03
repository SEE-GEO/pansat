"""
pansat.formats.hdf5
===================

This module extends the ``File`` class from the ``h5py`` module to provide
access datasets via the ``getattr`` function, which is required by the
Product description interface to turn HDF5 files into xarray Datasets.
"""
import weakref
import numpy as np

try:
    from h5py import File
except ImportError as error:
    print("The h5py package is required to read HDF5 files. Please install it.")
    raise error


class HDF5File(File):
    """
    Wrapper class for the ``h5py.File`` class that provides access to datasets
    via the ``getattribute`` method, which is used by the product description
    interface.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __getattr__(self, attr):
        value = File.__getitem__(self, attr)
        return value
