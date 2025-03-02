"""
pansat.products.satellite.himawari
===================================

This module defines the Himawari product class, which is used to represent all
products from the Himawari series of geostationary satellites.
"""
import re
from datetime import datetime
from pathlib import Path

import xarray

import pansat.download.providers as providers
from pansat.products.product import Product
from pansat.exceptions import NoAvailableProvider


class HimawariProduct(Product):
    """
    Base class for products from any of the currently operational
    Himawari satellites (GOES 16 and 17).

    Attributes:
        series_index(``int``): Index identifying the Himawari satellite
            in the Himawari seris.
        level(``int``): The operational level of the product.
        name(``str``): The name of the product.
        channel(``int``): The channel index.
    """

    def __init__(self, series_index, channel):
        self.series_index = series_index
        self.channel = channel
        if type(channel) == list:
            channels = "B(" + "|".join([f"{c:02}" for c in channel]) + ")"
        else:
            channels = f"B{channel:02}"

        self.filename_regexp = re.compile(
            rf"HS_H{self.series_index:02}_(\d{{8}})_(\d{{4}})_{channels}_FLDK_R\d\d_S\d{{4}}.DAT.bz2"
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
            filename(``str``): Filename of a GOES product.

        Returns:
            ``datetime`` object representing the timestamp of the
                filename.
        """
        path = Path(filename)
        match = self.filename_regexp.match(path.name)
        date_string = match.group(1) + match.group(2)
        date = datetime.strptime(date_string, "%Y%m%d%H%M")
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
        The default destination for GOES product is
        ``GOES-<index>/<product_name>``>
        """
        return Path(f"Himawari-{self.series_index:02}")

    def __str__(self):
        """The full product name."""
        return f"AHI-L1b-FLDK"

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

        if isinstance(self.channel, list):
            files = []
            for c in self.channel:
                prod = HimawariProduct(
                    self.series_index, c
                )
                p = provider(prod)
                files += p.download(start_time, end_time, destination)
            return files

        provider = provider(self)
        return provider.download(start_time, end_time, destination)

    def open(self, filename):
        """
        Open file as xarray dataset.

        Args:
            filename(``pathlib.Path`` or ``str``): The GOES file to open.
        """
        return xarray.open_dataset(filename)


l1b_himawari8_all = HimawariProduct(
    8,
    list(range(1, 17))
)
