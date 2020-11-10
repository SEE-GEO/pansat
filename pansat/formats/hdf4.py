"""
pansat.formats.hdf4
===================

This module provides a wrapper for the ``pyhdf`` package to simplify reading
of HDF4 files. The main interface to read a HDF4 file is implemented by the
``HDF4File`` class. The ``Dataset`` and ``VData`` classes represent the types
of variables that a HDF4 file contains.

Example
-------

With the ``HDF4File`` class reading HDF4 files and accessing their variables is
as simple as shown below:

.. code-block::

    file = HDF4File("file.hdf")
    print(file.variables)      # Print all variables in file.
    data = file.variable_1[:]  # Read data from variable named `variable_1`

"""
from dataclasses import dataclass
import weakref
import numpy as np

try:
    from pyhdf.HDF import HDF
    from pyhdf.SD import SD
    from pyhdf.VS import VS
except ImportError as error:
    print("The pyhdf package is required to read HDF4 file. Pleas install it.")
    raise error


@dataclass
class VData:
    """
    Class representing VData objects, i.e. numeric data that is stored in table
    format in an HDF file and accessed through the VS interface.

    Attributes:
        file(``weakref``): Weak reference to file object required for data
            access.
        name(``str``): The name of the attribute
        cls(``str``): The attribute class
        reference(``int``): Reference number identifying the vdata object.
        n_records(``int``): The number of records, i.e. rows, of the data table.
        n_fields(``int``): The number of fields, i.e. columns, of the
            data table.
        n_attributes(``int``): The number of attributes.
        size(``int``): Size of a single record (row) in bytes.
        tag(``int``): The vdata tag number.
        interlace(``int``): The vdata interlace mode.
    """

    file: weakref
    name: str
    cls: str
    reference: int
    n_records: int
    n_fields: int
    n_attributes: int
    size: int
    tag: int
    interlace: int

    def __str__(self):
        return f"HDF4 VData object: {self.name}, records={self.n_records}"

    def __getitem__(self, *args):
        """
        Selects datasets from file and forwards call to the returned vdata
        object.
        """
        data = self.file().vdata_table.attach(self.name).__getitem__(*args)
        return np.array(data)


@dataclass
class Dataset:
    """
    Class representing HDF4 Datasets, i.e. numeric data that is stored as
    multi-dimensional array and accessed through the SD interface.

    Attributes:
        file(``weakref``): Weak reference to file object required for data
            access.
        name(``str``): The name of the dataset.
        dimensions(``tuple``): Tuple containing the variable names of the dimensions
            holding the dimensions of the dataset.
        shape(``tuple``): Tuple containing the shape of the dataset.
        hdf_type(``int``): Integer representing the HDF-internal type of the dataset
        index(``int``): Integer representing the HDF-internal index of the dataset.
    """

    file: weakref
    name: str
    dimensions: tuple
    shape: tuple
    hdf_type: int
    index: int

    def __str__(self):
        return f"HDF4 Dataset object: {self.name}"

    def __getitem__(self, *args):
        """
        Selects datasets from file and forwards call to the returned dataset
        object.
        """
        return self.file().scientific_dataset.select(self.name).__getitem__(*args)


class HDF4File:
    """
    Simplified interface for reading HDF4 files. It combines the SD and VS low-level
    interfaces.

    Attributes:
        variables(``list``): List of strings of variable names contained in
            this file.
    """

    def __init__(self, path):
        self.path = path
        self.file_handle = HDF(str(path))

        self.scientific_dataset = SD(str(path))
        datasets = self.scientific_dataset.datasets()
        dataset_dict = {
            key: Dataset(weakref.ref(self), key, *info)
            for key, info in datasets.items()
        }
        self.datasets = dataset_dict

        self.vdata_table = VS(self.file_handle)
        vdata_dict = {
            info[0]: VData(weakref.ref(self), *info)
            for info in self.vdata_table.vdatainfo()
        }
        self.vdata = vdata_dict

    def __del__(self):
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None

    @property
    def variables(self):
        """
        Names of the variables available in this file.
        """
        return list(self.datasets.keys()) + list(self.vdata.keys())

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except AttributeError as error:
            datasets = object.__getattribute__(self, "datasets")
            if name in datasets:
                return datasets[name]
            vdata = object.__getattribute__(self, "vdata")
            if name in vdata:
                return vdata[name]
            raise error

    def __repr__(self):
        return f"HDF4File({self.path})"

    def to_xarray(self, product_description):

        dimensions = {dimension.name for dimension in product_description.dimensions}
        dims_with_coords = {
            dim.name for dim in coord.dimensions for coord in product_description.coords
        }
