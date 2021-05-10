"""
pansat.products.satellite.calipso
=================================

This module defines the Calipso product class, which represents all
supported Calipso products.
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


class CalipsoProduct(Product):
    """
    The Calipso class defines a generic interface for Calipso products.

    Attributes:
        name(``str``): The name of the product
        description(``list``): List of variable names provided by this
            product.
    """

    def __init__(self, name, description):
        self.name = name
        self._description = description
        self.filename_regexp = re.compile(
            r"CAL_LID_L2_"
            + name
            + r"-[\w]*-V[\d]?-[\d]{2}.([\d]{4})-([\d]{2})-([\d]{2})T([\d]{2})"
            + r"-([\d]{2})-([\d]{2})ZN\.*"
        )

    @property
    def description(self):
        # Product description object describing the Calipso product.
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
            filename(``str``): Filename of a Calipso product.

        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """
        filename = os.path.basename(filename)
        filename = filename.split(".")[1][:-2]
        return datetime.strptime(filename, "%Y-%m-%dT%H-%M-%S")

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
        The default destination for Calipso product is
        ``Calipso/<product_name>``>
        """
        return Path("Calipso") / Path(self.name)

    def __str__(self):
        """The full product name."""
        return "Calipso_" + self.name

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
            filename(``pathlib.Path`` or ``str``): The Calipso file to open.
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
            globals()[python_name] = CalipsoProduct(product_name, description)


_parse_products()
