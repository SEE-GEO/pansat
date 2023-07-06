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
import json
from pathlib import Path

import numpy as np
import xarray


class InconsistentDimensionsError(Exception):
    """
    This error is raised when inconsistent values have been deduced for
    the length of a Dimension object.
    """


class Dimension:
    """
    Represents a dimension of multi-dimensional dataset.


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
                f"Dimension  definition for '{self.name}'" " has no 'dimensions' field."
            )
        self.dimensions = json.loads(config_dict["dimensions"])
        self.unit = config_dict.get("unit", "")
        self.description = config_dict.get("description", "")
        self.callback = config_dict.get("callback", [])

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

    def get_data(self, file_handle, context, slc=None):
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
        if self.callback:
            callback = context[self.callback]
            data = callback(getattr(file_handle, self.field_name))
        else:
            if slc is None:
                slc = slice(0, None)
            data = getattr(file_handle, self.field_name)[slc]
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
        self.dimensions = []
        self.coordinates = []
        self.variables = []
        self.attributes = []
        self.callback = None
        self._name = ""
        self.latitude_coordinate = None
        self.longitude_coordinate = None
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
                self.dimensions.append(Dimension(section_name, section))
            elif section_type == "coordinate":
                self.coordinates.append(Variable(section_name, section))
            elif section_type == "latitude_coordinate":
                self.coordinates.append(Variable(section_name, section))
                self.latitude_coordinate = self.coordinates[-1]
            elif section_type == "longitude_coordinate":
                self.coordinates.append(Variable(section_name, section))
                self.longitude_coordinate = self.coordinates[-1]
            elif section_type == "variable":
                self.variables.append(Variable(section_name, section))
            elif section_type == "attribute":
                self.attributes.append(Variable(section_name, section))
            elif section_type == "callback":
                self.callback = section.get("callback", None)
            else:
                raise UnknownTypeError(
                    "Type should be one of ['dimension', "
                    "'coordinate', 'variable', 'latitude_coordinate', "
                    f"'longitude_coordinate'] but is '{section_type}'."
                )

    def _parse_properties(self, section_name, section):
        if "name" not in section:
            name = section_name
        else:
            name = section["name"]
        self._name = name
        self.properties = section

    @property
    def name(self):
        # The name of the data product.
        return self._name

    def _get_data(self, file_handle, context):
        """
        Reads dimensions, variables and coordinates from file_handle and
        returns them as dictionaries.

        Args:
            file_handle: File handle that provides access to the dimensions
                 and variable in this product description.

        Returns:
            A tuple ``(variables, coordinates)`` containing a two dictionaries.
            The ``variables`` maps variable names to 2-tuples of dimensionlists
            and data arrays. The ``coordinates`` dict maps coordinate names
            to dimension names or to tuples of dimension lists and data arrays.

        """
        variables = {}
        coordinates = {}
        attributes = {}
        for variable in self.variables:
            data = variable.get_data(file_handle, context)
            if len(variable.dimensions) < len(data.shape):
                data = np.squeeze(data)
            for index, dimension in enumerate(variable.dimensions):
                coordinates[dimension] = np.arange(data.shape[index])
            attrs = variable.get_attributes(file_handle)
            variables[variable.name] = (variable.dimensions, data, attrs)
        for coordinate in self.coordinates:
            data = coordinate.get_data(file_handle, context)
            if len(coordinate.dimensions) < len(data.shape):
                data = np.squeeze(data)
            attrs = coordinate.get_attributes(file_handle)
            coordinates[coordinate.name] = (coordinate.dimensions, data, attrs)
        for attribute in self.attributes:
            value = attribute.get_data(file_handle, context)
            attributes[attribute.name] = value

        return variables, coordinates, attributes

    def to_xarray_dataset(self, file_handle, context=None):
        """
        Convert data from file handle to xarray dataset.

        Args:
             file_handle: A file object providing access to the data product
                 described by this product description object.
        Return:
             ``xarray.Dataset`` containing the data from the provided file
             handle.
        """
        if not context:
            context = {}
        variables, dimensions, attributes = self._get_data(file_handle, context)
        dataset = xarray.Dataset(
            data_vars=variables, coords=dimensions, attrs=attributes
        )

        if self.callback is not None:
            callback = context[self.callback]
            callback(dataset, file_handle)

        return dataset

    def load_lonlats(self, file_handle, slc):
        """
        Load longitude and latitude coordinates from a file.

        Args:
            file_hanlde: File handle pointing to the file from which to load
                longitude and latitude coordinates.
            slc: A slice object to subset the loaded coodinates.

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
        lons = self.longitude_coordinate.get_data(file_handle, None, slc)
        lats = self.latitude_coordinate.get_data(file_handle, None, slc)
        return lons, lats
