"""
pansat.products.reanalysis.ncep
===================================
This module defines the NCEP reanalysis product class, which represents all
supported NCEP reanalysis products.


"""

import xarray
import re
import os
from datetime import datetime, timedelta
from pathlib import Path
import pansat.download.providers as providers
from pansat.products.product import Product


class NoAvailableProviderError(Exception):
    """
    Exception indicating that no suitable provider could be found for
    a product.
    """


class NCEPReanalysis(Product):
    """
    The NCEP reanalysis class defines a generic interface for NCEP products.

    Attributes:
        variable(``str``): Variable to extract
        grid(``str``): pressure, surface, spectral, surface_gauss or tropopause
        name(``str``): Full name of the product.
    """

    def __init__(self, variable, grid):
        self.variable = variable
        if grid == "tropopause":
            self.variable = variable + ".tropp"
        self.name = "ncep.reanalysis-" + str(grid)
        self.filename_regexp = re.compile(self.variable + ".*" + r".nc")

    def variable(self):
        return self._variable

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
            filename(``str``): Filename of a ERA5 product.
        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """
        filename = os.path.basename(filename)
        filename = filename.split(".")[-2]
        pattern = "%Y"

        return datetime.strptime(filename, pattern)

    def _get_provider(self):
        """ Find a provider that provides the product. """
        available_providers = [
            p
            for p in providers.ALL_PROVIDERS
            if str(self) in p.get_available_products()
        ]
        if not available_providers:
            raise NoAvailableProviderError(
                f"Could not find provider for the" f" product {self.name}."
            )
        return available_providers[0]

    @property
    def default_destination(self):
        """
        The default destination for ERA5 product is
        ``ERA5/<product_name>``>
        """
        return Path("NCEP") / Path(self.name)

    def __str__(self):
        """ The full product name. """
        return self.name

    def download(self, t0, t1, destination=None, provider=None):
        """
        Download data product for given time range.

        Args:
            start_time(``datetime``): ``datetime`` object defining the start date
                 of the time range.
            end_time(``datetime``): ``datetime`` object defining the end date of the
                 of the time range.
            destination(``str`` or ``pathlib.Path``): The destination where to store
                 the output data.

        Returns:

        downloaded(``list``): ``list`` with names of all downloaded files for respective data product

        """

        if not provider:
            provider = self._get_provider()

        if not destination:
            destination = self.default_destination
        else:
            destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        provider = provider(self)

        downloaded = provider.download(t0, t1, destination)

        return downloaded

    def open(self, filename):
        """Opens a given file of ERA5 product class as xarray.

        Args:
        filename(``str``): name of the file to be opened

        Returns:

        xr(``xarray.Dataset``): xarray dataset object"""
        xr = xarray.open_dataset(filename)

        return xr
