"""
pansat.products.satellite.goes16
================================

This module defines the GOES16 product class, which is used to represent all
GOES16 products.
"""
import re
from datetime import datetime
from pathlib import Path

import pansat.download.providers as providers
from pansat.products.product import Product


class NoAvailableProviderError(Exception):
    """
    Exception indicating that no suitable provider could be found for
    a product.
    """


class GOES16Product(Product):
    """
    Base class for GOES16 product.
    """

    def __init__(self, level, name, region, channel):
        self.level = level
        self.name = name
        self.region = region
        self.channel = channel
        if type(channel) == list:
            channels = "(" + "|".join([f"{c:02}" for c in channel]) + ")"

            self.filename_regexp = re.compile(
                rf"OR_ABI-L{self.level}-{self.name}{self.region}-\w\wC{channels}"
                r"_\w\w\w_s(\d*)_e(\d*)_c(\d*).nc"
            )
        else:
            self.filename_regexp = re.compile(
                rf"OR_ABI-L{self.level}-{self.name}{self.region}-\w\wC"
                rf"({self.channel:02})_\w\w\w_s(\d*)_e(\d*)_c(\d*).nc"
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
        print("MATCHING:", filename, self.filename_regexp.match(filename))
        return self.filename_regexp.match(filename)

    def filename_to_date(self, filename):
        """
        Extract timestamp from filename.

        Args:
            filename(``str``): Filename of a CloudSat product.

        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """
        path = Path(filename)
        match = self.filename_regexp.match(path.name)
        date_string = match.group(2)[:-1]
        date = datetime.strptime(date_string, "%Y%j%H%M%S")
        return date

    def _get_provider(self):
        """ Find a provider that provides the product. """
        available_providers = [
            p
            for p in providers.ALL_PROVIDERS
            if str(self) in p.get_available_products()
        ]
        if not available_providers:
            raise NoAvailableProviderError(
                f"Could not find a provider for the" f" product {self.name}."
            )
        return available_providers[0]

    @property
    def default_destination(self):
        """
        The default destination for CloudSat product is
        ``CloudSat/<product_name>``>
        """
        return Path("GOES16") / Path(str(self))

    def __str__(self):
        """ The full product name. """
        return f"ABI-L{self.level}-{self.name}{self.region}"

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
            filename(``pathlib.Path`` or ``str``): The CloudSat file to open.
        """
        return xarray.open_dataset(filename)

class GOES16L1BRadiances(GOES16Product):
    """
    Class representing GOES16 L1 radiance products.
    """
    def __init__(self, region, channel):
        super().__init__("1b", "Rad", region, channel)


l1b_radiances_c01_full_disk = GOES16L1BRadiances("F", 1)
l1b_radiances_c02_full_disk = GOES16L1BRadiances("F", 2)
l1b_radiances_c03_full_disk = GOES16L1BRadiances("F", 3)
l1b_radiances_c04_full_disk = GOES16L1BRadiances("F", 4)
l1b_radiances_all_full_disk = GOES16L1BRadiances("F", list(range(16)))
l1b_radiances_c01_conus = GOES16L1BRadiances("C", 1)
l1b_radiances_c02_conus = GOES16L1BRadiances("C", 2)
l1b_radiances_c03_conus = GOES16L1BRadiances("C", 3)
l1b_radiances_c04_conus = GOES16L1BRadiances("C", 4)
l1b_radiances_all_conus = GOES16L1BRadiances("C", list(range(16)))
