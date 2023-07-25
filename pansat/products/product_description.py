"""
pansat.products.product_description
===================================

The ``product_description`` module provides a class to parse product
description files. Product descriptions are files in the ``.ini``
format that are used to describe data products. This is required for
data products that do not use the NetCDf format and thus need additional
information in order to read them into an ``xarray`` Dataset.

Example
-------

The example below provides a minimal example for a product description file.

.. code-block::

    [1B-CPR]
    type = properties
    name = 1B-CPR

    [rays]
    type = dimension

    [latitude]
    type = coordinate
    name = Latitude
    description = Spacecraft geodetic latitude.
    unit = degree
    dimensions = ["rays"]


A product description files consists of different sections, each of which
has a mandatory ``type`` defining its meaning. The following section
types are currently supported:

properties
----------

The ``properties`` section defines the general properties of the dataset.
So far, this section is used only to set the name of the product. The
title of the section is ignored.


dimension
---------

The ``dimension`` section defines a dimension used in the dataset. If
the section contains a field ``name``, then the variable of this name
will be used as numeric values defining the grid values along this
dimension. If this is not the case, ``pansat`` will automatically
try to infer the size of the dimension and use element indices
``[0, 1, ...]`` as grid values.


variable
--------

The ``variable`` section defines a multi-dimensional data array. A ``variable``
section must have fields ``name`` and ``dimensions``, which contain the name of
the corresponding data variable in a file of the product and a list of
corresponding dimensions names, respectively. Note that the dimension names
must all be defined in the same product description file.

Optionally, the ``variable`` section may contain the following additional
entries:

- ``unit``: The unit in which the data is given.
- ``description``: A string description of the variable
- ``callback``: Name of a function that is called on the corresponding
  attribute of a product file instead of trying to access the data directly
  through the attribute. This allows customized pre-processing of variable
  data.


coordinate
----------

For ``coordinate`` sections, the same rules apply as for ``variable`` section.
The difference between a coordinate and a variable is that variables are mapped
to xarray Datasets, whereas coordinates are mapped to coordinates.

Reference
---------
"""
from configparser import ConfigParser
from dataclasses import dataclass
import json
from pathlib import Path

import numpy as np
import xarray

from pansat.geometry import LonLatRect
from pansat.time import TimeRange


class InconsistentDimensionsError(Exception):
    """
    This error is raised when inconsistent values have been deduced for
    the length of a Dimension object.
    """


class Dimension:
    """
    Represents a dimension of a multi-dimensional dataset.
    """

    def __init__(self, name, config_dict):
        """
        Create dimension object from ``.ini`` file section.

        Args:
            name(``str``): The name of the ``.ini`` file section describing the
                the dimension.
            config_dict: The dictionary describing the section of the ``.ini``
                file.
        """
        self.name = name
        self.field_name = config_dict.get("name")
        self._size = None

    @property
    def size(self):
        """The size of the dimension."""
        return self._size

    def get_size(self, file_handle):
        """
        Return the size of a dimension.

        Args:
            file_handle: File handle pointing to an open file object
                containing the dataset.
        """
        return getattr(file_handle, self.field_name).size

    @size.setter
    def size(self, value):
        if not self._size:
            self._size = value
        else:
            if not self._size == value:
                raise InconsistentDimensionsError(
                    "Deduced inconsistent" " dimensions for dimension" f" {self.name}."
                )

    def __repr__(self):
        return f"Dimension({self.name})"


class MissingFieldError(Exception):
    """
    This error is raised when a section of an ``.ini`` file lacks a required
    key.
    """


class Variable:
    """
    Base class for all variable-type product description entries. Variable-type
    entries in a product description represent a multi-dimensional data array,
    which has a name and associated dimensions. Optionally, it may have a unit
    and a description.
    """

    def __init__(self, name, config_dict):
        """
        Args:
            name(``str``): The section name of the ``.ini`` file. which is used
                as the variable name.
            config_dict(``dict``): The dict containing the keys from the
                ``.ini`` file.
        """
        self.name = name
        self._parse_config_dict(config_dict)

    def _parse_config_dict(self, config_dict):
        """Helper function to parse config dict from ``.ini`` file."""
        if "name" not in config_dict:
            raise MissingFieldError(
                f"Dimension  definition for '{self.name}' " "has no 'name' field."
            )
        self.field_name = config_dict["name"]
        if "dimensions" not in config_dict:
            raise MissingFieldError(
                f"Variable definition for '{self.name}'" " has no 'dimensions' field."
            )
        self.dimensions = json.loads(config_dict["dimensions"])
        self.unit = config_dict.get("unit", "")
        self.description = config_dict.get("description", "")
        self.callback = config_dict.get("callback", [])


    def _extract_slices(self, slcs):
        """
        Transforms the dictionary of slices given by 'slcs' to a tuple
        of slices matching the dimensions of the variable.

        Args:
            slcs: A dictionary mapping dimensions names to slice
                objects.

        Return:
            A tuple of slice object that can be used to load the data
            from this variable.
        """
        if slcs is None:
            slcs = {}
        return tuple(
            (slcs.get(name, slice(0, None)) for name in self.dimensions)
        )


    def get_attributes(self, file_handle):
        """
        Get dict of xarray attributes containing unit and description.
        """
        attributes = {}
        field = getattr(file_handle, self.field_name)
        if hasattr(field, "attrs"):
            for key, value in field.attrs.items():
                if isinstance(value, bytes):
                    value = value.decode()
                attributes[key] = value

        if self.unit:
            attributes["unit"] = self.unit
        if self.description:
            attributes["description"] = self.description

        return attributes

    def get_data(self, file_handle, context, slcs=None):
        """
        Get data for variable from file_handle.

        Retrieves data from given file handle by retrieving the attribute
        corresponding to the variables ``field_name`` attribute. If
        a callback is set for the variable, it is used to retrieve the
        data from the attribute otherwise the data is retrieved by calling
        __getitem__ with a single colon.

        Args:
             file_handle: Object providing access to a product file.
             context: Namespace in which to lookup the callback function.
             slc: A slice object to subset the loaded data.
        """
        if slcs is not None:
            slcs = self._extract_slices(slcs)
        else:
            slcs = slice(0, None)

        if self.callback:
            callback = context[self.callback]
            data = callback(getattr(file_handle, self.field_name), slcs)
        else:
            data = getattr(file_handle, self.field_name)[slcs]

        return data

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"


class UnknownTypeError(Exception):
    """
    This error is raised when an ``.ini`` file contains a section
    with an unknown type.
    """


class DimensionNameError(Exception):
    """
    This error is thrown when a dimension is requested that is not part
    of the product description.
    """


@dataclass
class GranuleInfo:
    """
    Holds information about the splitting of a data file into
    granules.
    """
    dimensions: list[str]
    partitions: list[int]

    def __init__(self, config_dict):
        if not "granule_dimensions" in config_dict:
            raise ValueError(
                "'properties' section must have a 'granule_dimensions' entry specifying the"
                "dimensions along which to partition the data."
            )
        self.dimensions = json.loads(config_dict["granule_dimensions"])

        if not "granule_partitions" in config_dict:
            partitions = [10] * len(self.dimensions)
        else:
            partitions = json.loads(config_dict["granule_partitions"])
        self.partitions = [int(part) for part in partitions]


class ProductDescription(ConfigParser):
    """
    This class represents a description of a data product which is parsed from
    a ``.ini`` file.

    Attributes:
        name(``str``): The name of the dataset.
        dimensions: List of the dimensions of the data product.
        coordinates: List of the coordinate-type data in the data product.
        variable: List of the variable-type data in the data product.
        attribute: List of the attribute-type data in the data product.
    """

    def __init__(self, ini_file):
        super().__init__()
        self.dimensions = {}
        self.coordinates = {}
        self.variables = {}
        self.attributes = {}
        self.callback = None
        self._name = ""
        self.latitude_coordinate = None
        self.longitude_coordinate = None
        self.time_coordinate = None
        try:
            self.read(ini_file)
        except OSError:
            self.read_string(ini_file)
        self._parse_config_file()

    def _parse_config_file(self):
        for section_name in self.sections():
            section = self[section_name]
            if "type" not in section:
                raise MissingFieldError(
                    f"Section {section_name} has no 'type'" " field."
                )
            section_type = section["type"].lower()
            if section_type == "properties":
                self._parse_properties(section_name, section)
            elif section_type == "dimension":
                self.dimensions[section_name] = Dimension(
                    section_name,
                    section
                )
            elif section_type == "coordinate":
                self.coordinates[section_name] = Variable(
                    section_name,
                    section
                )
            elif section_type == "latitude_coordinate":
                self.coordinates[section_name] = Variable(
                    section_name,
                    section
                )
                self.latitude_coordinate = self.coordinates[section_name]
            elif section_type == "longitude_coordinate":
                self.coordinates[section_name] = Variable(
                    section_name,
                    section
                )
                self.longitude_coordinate = self.coordinates[section_name]
            elif section_type == "time_coordinate":
                self.coordinates[section_name] = Variable(
                    section_name,
                    section
                )
                self.time_coordinate = self.coordinates[section_name]
            elif section_type == "variable":
                self.variables[section_name] = Variable(
                    section_name,
                    section
                )
            elif section_type == "attribute":
                self.attributes[section_name] = Variable(
                    section_name,
                    section
                )
            elif section_type == "callback":
                self.callback = section.get("callback", None)
            elif section_type == "granules":
                self.granules = GranuleInfo(section)
            else:
                raise UnknownTypeError(
                    "Type should be one of ['dimension', "
                    "'coordinate', 'variable', 'latitude_coordinate', "
                    f"'longitude_coordinate'] but is '{section_type}'."
                )

    def _parse_properties(self, section_name, section):
        """
        Parses the properties section of the product description file.

        Args:
            section_name: The name of the properties section
            section: The section object holding the
                 fields of the properties section.
        """
        if "name" not in section:
            name = section_name
        else:
            name = section["name"]
        self._name = name
        if "granule_dimensions" in section:
            self.granule_info = GranuleInfo(section)
        else:
            self.granule_info = None
        self.properties = section

    @property
    def name(self):
        # The name of the data product.
        return self._name

    def _get_data(self, file_handle, context, slcs=None):
        """
        Reads dimensions, variables and coordinates from file_handle and
        returns them as dictionaries.

        Args:
            file_handle: File handle that provides access to the dimensions
                 and variable in this product description.
             context: A Python context defining the callback functions
                 used to load the product data.
             slcs: An optional dictionary mapping dimension names to
                 slices to use to load only a subset of the data.

        Returns:
            A tuple ``(variables, coordinates)`` containing a two dictionaries.
            The ``variables`` maps variable names to 2-tuples of dimensionlists
            and data arrays. The ``coordinates`` dict maps coordinate names
            to dimension names or to tuples of dimension lists and data arrays.

        """
        variables = {}
        coordinates = {}
        attributes = {}
        for name, variable in self.variables.items():
            data = variable.get_data(file_handle, context, slcs=slcs)
            if len(variable.dimensions) < len(data.shape):
                data = np.squeeze(data)
            for index, dimension in enumerate(variable.dimensions):
                coordinates[dimension] = np.arange(data.shape[index])
            attrs = variable.get_attributes(file_handle)
            variables[name] = (variable.dimensions, data, attrs)
        for name, coordinate in self.coordinates.items():
            data = coordinate.get_data(file_handle, context, slcs=slcs)
            if len(coordinate.dimensions) < len(data.shape):
                data = np.squeeze(data)
            attrs = coordinate.get_attributes(file_handle)
            coordinates[name] = (coordinate.dimensions, data, attrs)
        for name, attribute in self.attributes.items():
            value = attribute.get_data(file_handle, context)
            attributes[name] = value

        return variables, coordinates, attributes

    def to_xarray_dataset(
            self,
            file_handle,
            context=None,
            slcs=None
    ):
        """
        Convert data from file handle to xarray dataset.

        Args:
             file_handle: A file object providing access to the data product
                 described by this product description object.
             context: A Python context defining the callback functions
                 used to load the product data.
             slcs: An optional dictionary mapping dimension names to
                 slices to use to load only a subset of the data.

        Return:
             ``xarray.Dataset`` containing the data from the provided file
             handle.
        """
        if not context:
            context = {}
        variables, dimensions, attributes = self._get_data(
            file_handle,
            context,
            slcs=slcs
        )
        dataset = xarray.Dataset(
            data_vars=variables, coords=dimensions, attrs=attributes
        )

        if self.callback is not None:
            callback = context[self.callback]
            callback(dataset, file_handle)

        return dataset


    def load_lonlats(self, file_handle, context=None, slcs=None):
        """
        Load longitude and latitude coordinates from a file.

        Args:
            file_hanlde: File handle pointing to the file from which to load
                longitude and latitude coordinates.
            slcs: A dictionary mapping dimension names to slices to
                subset the loaded coordinates.

        Return:
            A tuple ``(lons, lats)`` containing the loaded longitude and
            latitude coordinates as numpy arrays.
        """
        if self.latitude_coordinate is None or self.longitude_coordinate is None:
            raise ValueError(
                "Product description needs 'latitude_coordinate' and "
                "'latitude_coordinate' fields to extract latitude and "
                " coordinates."
            )
        lons = self.longitude_coordinate.get_data(file_handle, context, slcs)
        lats = self.latitude_coordinate.get_data(file_handle, context, slcs)
        return lons, lats

        return lons, lats


    def load_time(self, file_handle, context=None, slcs=None):
        """
        Load time coordinates from a file.

        Args:
            file_hanlde: File handle pointing to the file from which to load
                longitude and latitude coordinates.
            slcs: A dictionary mapping dimension names to slices to
                subset the loaded coordinates.

        Return:
            A tuple ``(lons, lats)`` containing the loaded longitude and
            latitude coordinates as numpy arrays.
        """
        if self.time_coordinate is None:
            raise ValueError(
                "Product description needs 'time_coordinate' fields to "
                " the time coordinates."
            )
        time = self.time_coordinate.get_data(file_handle, context, slcs)
        return time


    def get_granule_data(self, file_handle, context=None):
        """
        Extracts relevant granule data from a file handle.

        Args:
            file_handle: A file handle object providing access to a product
                data file.
            context: A Python context holding potential callback functions
                required for the loading of data.

        Return:
            A list of tuples ``(t_rng, geom, prm_ind_name, prm_ind_rng)``
            containing the time range, geometry and primary index name and
            range of each granule in the file.

            If the granuling takes place over two dimension each tuple
            additionally contains the name and range of the secondary
            index.
        """
        if self.granule_info is None:
            raise ValueError(
                "This product description does not contain any granule "
                "and can therefore not provide any granules."
            )
        if self.latitude_coordinate is None or self.longitude_coordinate is None:
            raise ValueError(
                "This product lacks  longitude and latitude coordinates and "
                "and can therefore not provide any granules."
            )
        if self.time_coordinate is None:
            raise ValueError(
                "This product lacks a time coordinate and "
                "and can therefore not provide any granules."
            )

        dim_names = self.granule_info.dimensions

        try:
            sizes = [self.dimensions[name].get_size(file_handle) for name in dim_names]
        except TypeError:
            lons, lats = self.load_lonlats(file_handle, context=context)
            time = self.load_time(file_handle, context=context)

            if len(time.shape) > len(lons.shape):
                sizes = time.shape[:len(dim_names)]
            else:
                sizes = lons.shape[:len(dim_names)]

        granule_data = []

        outer_start = 0
        while(outer_start < sizes[0]):
            outer_end = (min(
                outer_start + sizes[0] // self.granule_info.partitions[0],
                sizes[0],
            ))

            outer_slc = slice(
                outer_start,
                outer_end,
                outer_end - outer_start - 1
            )
            outer_start = outer_end

            if len(sizes) == 1:
                inner_stop = 1
            else:
                inner_stop = sizes[1]
            inner_start = 0

            while inner_start < inner_stop:

                if len(sizes) == 1:
                    slcs = {
                        dim_names[0]: outer_slc,
                    }
                    inner_start = inner_stop
                else:
                    inner_end = (min(
                        inner_start + sizes[1] // self.granule_info.partitions[1],
                        sizes[1],
                    ))
                    inner_slc = slice(
                        inner_start,
                        inner_end,
                        inner_end - inner_start - 1
                    )
                    slcs = {
                        dim_names[0]: outer_slc,
                        dim_names[1]: inner_slc,
                    }
                    inner_start = inner_end

                time = self.load_time(
                    file_handle,
                    context=context,
                    slcs=slcs
                )
                start_time = time.min()
                end_time = time.max()
                time_range = TimeRange(start_time, end_time)

                lons, lats = self.load_lonlats(
                    file_handle,
                    context=context,
                    slcs=slcs
                )
                geom = LonLatRect(
                    lons[0, 0],
                    lats[0, 0],
                    lons[-1, -1],
                    lats[-1, -1]
                )

                if len(dim_names) == 1:
                    granule_data.append((
                        time_range,
                        geom,
                        dim_names[0],
                        (outer_start, outer_end),
                    ))
                else:
                    granule_data.append((
                        time_range,
                        geom,
                        dim_names[0],
                        (outer_start, outer_end),
                        dim_names[1],
                        (inner_start, inner_end)
                    ))

        return granule_data


    def open_granule(self, file_handle, granule, context=None):
        """
        Open data from a granule.

        Args:
            file_handle: A file handle object providing access to a product
                data file.
            granule: A Granule object identifying the granule to load.
            context: A Python context holding potential callback functions
                required for the loading of data.

        Return:
            An xarray.Dataset containing the product data for the granule
            in question.
        """
