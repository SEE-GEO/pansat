"""
pansat.products.ground_based
============================

This module defines products provided by Cloudnet. The official
data portal for CloudNet is available
`here <https://cloudnet.fmi.fi/>`_.
"""
from datetime import datetime
from pathlib import Path
import re

from pansat.exceptions import NoAvailableProvider
from pansat.products.product import Product
from pansat.download import providers


class CloudnetProduct(Product):
    """
    This class represents all Cloudnet products. Since Cloudnet data
    is derived from specific stations a product can have an associated
    location in which case only data of the product collected at the
    given location is represetned by the product.
    """

    def __init__(self, product_name, description, location=None):
        """
        Args:
            product_name: The name of the product.
            description: A string describing the product.
            location: An optional string specifying the a specific
                 Cloudnet location.
        """
        self.product_name = product_name
        self._description = description
        self.location = location

        if location is not None:
            self.filename_regexp = re.compile(rf"(\d{{8}})_{location}_[-\w\d]*.nc")
        else:
            self.filename_regexp = re.compile(rf"(\d{{8}})_([\w-]*)_[-\w\d]*.nc")

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
            filename(``str``): Filename of a Cloudnet product.

        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """
        parts = filename.split("/")
        if len(parts) > 1:
            filename = parts[-1]
        path = Path(filename)
        match = self.filename_regexp.match(path.name)

        date_string = match.group(1)
        date = datetime.strptime(date_string, "%Y%m%d")
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
                f"Could not find a provider for the" f" product {self}."
            )
        return available_providers[0]

    @property
    def default_destination(self):
        """
        The default location for Cloudnet products is cloudnet/<product_name>
        """
        return Path("cloudnet") / Path(self.product_name)

    def __str__(self):
        s = f"ground_based::Cloudnet::{self.product_name}"
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
            filename(``pathlib.Path`` or ``str``): The Cloudnet file to open.
        """
        return xr.load_dataset(filename)


l2_iwc = CloudnetProduct("iwc", "IWC calculated from Z-T method.")
l1_radar = CloudnetProduct("radar", "L1b radar data.")
