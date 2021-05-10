"""
pansat.products.ground_based.opera
==================================

This module defines the ``OperaProduct`` class, which is used to represent data
products from the EUMETNET Opera ground-radar network.
"""
import datetime
from pathlib import Path
import re

import numpy as np
import pyproj

import pansat.download.providers as providers
from pansat.products.product import Product
from pansat.products.product_description import ProductDescription
from pansat.exceptions import NoAvailableProvider


class OperaProduct(Product):
    """
    Class representing Opera products.

    OPERA is the Radar Programme of EUMETNET. More information can be found on
    the `EUMETNET <https://www.eumetnet.eu/activities/observations-programme/current-activities/opera/>`_ homepage.
    """

    def __init__(self, product_name, description):
        self.product_name = product_name
        self._description = description
        self.filename_regexp = re.compile(
            rf"OPERA_{self.product_name}_(\d{{4}})_(\d{{3}})"
            rf"_(\d{{2}})_(\d{{2}})\.hdf"
        )

    @property
    def variables(self):
        return []

    @property
    def description(self):
        return self._description

    def matches(self, filename):
        """
        Determines whether a given filename matches the pattern used for
        the product.

        Args:
            filename(``str``): The filename

        Return:
            True if the filename matches the product, False otherwise.
        """
        return self.filename_regexp.match(filename)

    def filename_to_date(self, filename):
        """
        Extract timestamp from filename.

        Args:
            filename(``str``): Filename of an Opera product.

        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """
        path = Path(filename)
        match = self.filename_regexp.match(path.name)
        date_string = match.group(1) + match.group(2) + match.group(3) + match.group(4)
        date = datetime.datetime.strptime(date_string, "%Y%j%H%M")
        return date

    def _get_provider(self):
        """Find a provider that provides the product."""
        available_providers = [
            p
            for p in providers.ALL_PROVIDERS
            if str(self) in p.get_available_products()
        ]
        if not available_providers:
            raise NoAvailableProvider(
                f"Could not find a provider for the" f" product {self.name}."
            )
        return available_providers[0]

    @property
    def default_destination(self):
        """
        The default destination for Opera product is
        ``Opera/<product_name>``>
        """
        return Path("Opera") / Path(self.product_name)

    def __str__(self):
        s = f"OPERA_{self.product_name}"
        return s

    def download(self, start_time, end_time, destination=None, provider=None):
        """
        Download data product for given time range.

        Args:
            start_time(``datetime``): ``datetime`` object defining the start
                 date of the time range.
            end_time(``datetime``): ``datetime`` object defining the end date
                 of the of the time range.
            destination(``str`` or ``pathlib.Path``): The destination where to
                 store the output data.
        """

        if not provider:
            provider = self._get_provider()

        if not destination:
            destination = self.default_destination
        else:
            destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        provider = provider(self)

        return provider.download(start_time, end_time, destination)

    def open(self, filename):
        """
        Open file as xarray dataset.

        Args:
            filename(``pathlib.Path`` or ``str``): The Opera file to open.
        """
        from pansat.formats.hdf5 import HDF5File

        file_handle = HDF5File(filename, "r")
        return self.description.to_xarray_dataset(file_handle, globals())


def edges_to_centers(grid):
    new_grid = 0.25 * (grid[1:, 1:] + grid[1:, :-1] + grid[:-1, 1:] + grid[:-1, :-1])
    return new_grid


_LATITUDE_GRID = None
_LONGITUDE_GRID = None


def _define_coordinate_grids():
    projection_string = (
        "+proj=laea +lat_0=55.0 +lon_0=10.0 +x_0=1950000.0 "
        "+y_0=-2100000.0 +units=m +ellps=WGS84"
    )
    proj = pyproj.Proj(projection_string)
    x = np.arange(0, 1900 * 2000.0 + 1, 2000.0)
    y = np.arange(0, -2200 * 2000.0 - 1, -2000.0)
    x, y = np.meshgrid(x, y)
    lons, lats = proj(x.ravel(), y.ravel(), inverse=True)
    lats = lats.reshape(2201, 1901)
    lons = lons.reshape(2201, 1901)

    global _LATITUDE_GRID
    global _LONGITUDE_GRID
    _LATITUDE_GRID = edges_to_centers(lats)
    _LONGITUDE_GRID = edges_to_centers(lons)
    _LATITUDE_GRID.flags.writeable = False
    _LONGITUDE_GRID.flags.writeable = False


def _get_opera_projection():
    projection_string = (
        "+proj=laea +lat_0=55.0 +lon_0=10.0 +x_0=1950000.0 "
        "+y_0=-2100000.0 +units=m +ellps=WGS84"
    )
    proj = pyproj.Proj(projection_string)
    return proj


def get_latitude_grid(*args):
    """
    Returns the latitude grid on which the Opera data is provided.
    """
    global _LATITUDE_GRID
    if _LATITUDE_GRID is None:
        _define_coordinate_grids()
    return _LATITUDE_GRID


def get_longitude_grid(*args):
    """
    Returns the longitude grid on which the Opera data is provided.
    """
    global _LONGITUDE_GRID
    if _LONGITUDE_GRID is None:
        _define_coordinate_grids()
    return _LONGITUDE_GRID


def _parse_products():
    """
    Parses available Opera products.
    """
    module_path = Path(__file__).parent
    for filename in module_path.iterdir():
        if filename.match("*.ini"):
            description = ProductDescription(filename)
            python_name = filename.name.split(".")[0]
            product_name = description.name
            globals()[python_name] = OperaProduct(product_name, description)


_parse_products()
