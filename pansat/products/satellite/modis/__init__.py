"""
pansat.products.satellite.modis
===============================

This module define the MODIS product class, which is used to represent all MODIS
products.
"""
import re
from datetime import datetime
from pathlib import Path

import pansat.download.providers as providers
from pansat.products.product import Product
from pansat.exceptions import NoAvailableProvider


class MODISProduct(Product):
    """
    Base class for MODIS product.
    """

    def __init__(self, product_name):
        self.product_name = product_name
        self.filename_regexp = re.compile(
            rf"{self.product_name.upper()}\.A(\d{{7}})\.(\d{{4}}).\d{{3}}.\d*\.hdf"
        )

    @property
    def variables(self):
        return []

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
            filename(``str``): Filename of a MODIS product.

        Returns:
            ``datetime`` object representing the timestamp of the
                filename.
        """
        path = Path(filename)
        match = self.filename_regexp.match(path.name)
        date_string = match.group(1) + match.group(2)
        date = datetime.strptime(date_string, "%Y%j%H%M")
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
        The default destination for CloudSat product is
        ``MODIS/<product_name>``>
        """
        return Path("MODIS")

    def __str__(self):
        """The full product name."""
        platform = "Terra"
        if self.product_name[:2] == "MY":
            platform = "Aqua"
        return f"MODIS_{platform}_{self.product_name}"

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
            filename(``pathlib.Path`` or ``str``): The MODIS file to open.
        """
        raise Exception("Currently not implemented.")


modis_terra_1km = MODISProduct("MOD021KM")
modis_terra_geo = MODISProduct("MOD03")
modis_terra_cloud_mask = MODISProduct("MOD35_l2")
modis_aqua_1km = MODISProduct("MYD021KM")
modis_aqua_geo = MODISProduct("MYD03")
modis_aqua_cloud_mask = MODISProduct("MYD35_l2")
