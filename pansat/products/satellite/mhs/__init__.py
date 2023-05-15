"""
pansat.products.satellite.mhs
=============================

This module defines the MHS product group which comprises observations from
any of the MHS sensors.
"""
import re
from datetime import datetime
from pathlib import Path

import numpy as np

import pansat.download.providers as providers
from pansat.products.product import Product
from pansat.products.product_description import ProductDescription
from pansat.exceptions import NoAvailableProvider


class MHSProduct(Product):
    """
    Base class representing MHS products.
    """

    def __init__(self):
        self.filename_regexp = re.compile(
            "MHSx_xxx_1B_M01_(\d{8})Z_(\d{8})Z_N_O_\d{8}Z.nat"
        )
        self.name = "MHS_L1B"

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
            filename(``str``): Filename of a GPM product.

        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """
        path = Path(filename)
        match = self.filename_regexp.match(path.name)
        date_string = match.group(2) + match.group(3)
        date = datetime.strptime(date_string, "%Y%m%d%H%M%S")
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
                f"Could not find a provider for the" f" product {str(self)}."
            )
        return available_providers[0]

    @property
    def default_destination(self):
        """
        The default destination for GPM product is
        ``GPM/<product_name>``>
        """
        return Path("MHS") / Path(self.name)

    def __str__(self):
        return "MHS_L1B"

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
            filename(``pathlib.Path`` or ``str``): The GPM file to open.
        """
        from pansat.formats.hdf5 import HDF5File

        file_handle = HDF5File(filename, "r")
        return self.description.to_xarray_dataset(file_handle, globals())


l1b_mhs = MHSProduct()
