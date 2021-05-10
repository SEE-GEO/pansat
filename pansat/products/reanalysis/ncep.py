"""
pansat.products.reanalysis.ncep
===============================
This module defines the NCEP reanalysis product class, which represents all
supported NCEP reanalysis products.


"""

import re
import os
from datetime import datetime
from pathlib import Path

import xarray
import pansat.download.providers as providers
from pansat.products.product import Product
from pansat.exceptions import NoAvailableProvider


class NCEPReanalysis(Product):
    """
    The NCEP reanalysis class defines a generic interface for NCEP products.

    Attributes:
        variable(``str``): Variable to extract
        grid(``str``): pressure, surface, spectral, surface_gauss or tropopause
        name(``str``): Full name of the product.
    """

    def __init__(self, variable, grid):
        """
        Args:
            variable(``str``): Variable to extract
            grid(``str``): pressure, surface, spectral, surface_gauss or tropopause
        """

        self.variable = variable
        if grid == "tropopause":
            self.variable = variable + ".tropp"
        self.name = "ncep.reanalysis-" + str(grid)
        self.filename_regexp = re.compile(self.variable + ".*" + r".nc")

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
            filename(``str``): Filename of a NCEP product.

        Returns:
            ``datetime`` object representing the timestamp of the
                filename.
        """
        filename = os.path.basename(filename)
        filename = filename.split(".")[-2]
        pattern = "%Y"

        return datetime.strptime(filename, pattern)

    def _get_provider(self):
        """Find a provider that provides the product."""
        available_providers = [
            p
            for p in providers.ALL_PROVIDERS
            if str(self) in p.get_available_products()
        ]
        if not available_providers:
            raise NoAvailableProvider(
                f"Could not find provider for the product {self.name}."
            )
        return available_providers[0]

    @property
    def default_destination(self):
        """
        The default destination for NCEP product is
        ``NCEP/<product_name>``>
        """
        return Path("NCEP") / Path(self.name)

    def __str__(self):
        """The full product name."""
        return self.name

    def download(self, start, end, destination=None):
        """
        Download data product for given time range.

        Args:
            start(``int``): start year
            end(``int``): end year
            destination(``str`` or ``pathlib.Path``): The destination where to store
                the output data.

        Returns:
            downloaded(``list``): name list of all downloaded files for data product

        """

        provider = self._get_provider()

        if not destination:
            destination = self.default_destination
        else:
            destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        provider = provider(self)

        downloaded = provider.download(start, end, destination)

        return downloaded

    @classmethod
    def open(cls, filename):
        """Opens a given file of NCEP product class as xarray.

        Args:
            filename(``str``): name of the file to be opened

        Returns:
            datasets(``xarray.Dataset``): xarray dataset object for opened file
        """

        datasets = xarray.open_dataset(filename)

        return datasets
