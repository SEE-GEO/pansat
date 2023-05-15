"""
pansat.products.satellite.dardar
===================================

This module defines the Dardar product class, which represents all
supported Dardar products.
"""
import re
import os
from datetime import datetime
from pathlib import Path

import numpy as np

from pansat.products.product_description import ProductDescription
import pansat.download.providers as providers
from pansat.products.product import Product
from pansat.exceptions import NoAvailableProvider


class DardarProduct(Product):
    """
    The Dardar class defines a generic interface for Dardar products.

    Attributes:
        name(``str``): The name of the product
        description(``list``): List of variable names provided by this
            product.
    """

    def __init__(self, name, description, version):
        self.name = name
        self._description = description
        self.version = version
        if self.version < 3:
            self.filename_regexp = re.compile(
                # DARDAR-CLOUD_v2.1.1_2014015035336_41054.hdf
                name.replace("_", "-")
                + r"_v[\d]\.[\d]\.[\d]_[\d]*_[\d]*\.hdf"
            )
        else:
            self.filename_regexp = re.compile(
                # DARDAR-CLOUD_2009001003637_14252_V3-00.nc
                name.replace("_", "-")
                + r"_[\d]*_[\d]*_V\d-\d\d.nc"
            )

    @property
    def description(self):
        # Product description object describing the Dardar product.
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
            filename(``str``): Filename of a Dardar product.

        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """
        filename = os.path.basename(filename)
        if self.version < 3:
            filename = filename.split("_")[2]
        else:
            filename = filename.split("_")[1]
        return datetime.strptime(filename, "%Y%j%H%M%S")

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
        The default destination for Dardar product is
        ``Dardar/<product_name>``>
        """
        return Path("Dardar") / Path(self.name)

    def __str__(self):
        """The full product name."""
        return "Dardar_" + self.name

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
            filename(``pathlib.Path`` or ``str``): The Dardar file to open.
        """
        from pansat.formats.hdf4 import HDF4File

        file_handle = HDF4File(filename)
        return self.description.to_xarray_dataset(file_handle, globals())


def _parse_products():
    module_path = Path(__file__).parent
    for filename in module_path.iterdir():
        if filename.match("*.ini"):
            description = ProductDescription(filename)
            python_name = filename.name.split(".")[0]
            product_name = description.name
            version = float(description.properties["version"])
            globals()[python_name] = DardarProduct(product_name, description, version)


_parse_products()
