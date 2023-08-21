"""
pansat.products.satellite.goes
==============================

This module defines the GOES product class, which is used to represent all
products from the GOES series of geostationary satellites.
"""
import re
from datetime import datetime
from pathlib import Path

import xarray

import pansat.download.providers as providers
from pansat.products.product import Product
from pansat.exceptions import NoAvailableProvider


class GOESProduct(Product):
    """
    Base class for products from any of the currently operational
    GOES satellites (GOES 16 and 17).

    Attributes:
        series_index(``int``): Index identifying the GOES satellite
            in the GOES seris.
        level(``int``): The operational level of the product.
        name(``str``): The name of the product.
        channel(``int``): The channel index.
    """

    def __init__(self, series_index, level, name, region, channel):
        self.series_index = series_index
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
        date_string = match.group(2)[:-1]
        date = datetime.strptime(date_string, "%Y%j%H%M%S")
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
        return Path(f"GOES-{self.series_index}") / Path(str(self))

    def __str__(self):
        """The full product name."""
        return f"GOES-{self.series_index}-ABI-L{self.level}-{self.name}{self.region}"

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
                prod = GOESProduct(
                    self.series_index, self.level, self.name, self.region, c
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


class GOES16L1BRadiances(GOESProduct):
    """
    Class representing GOES16 L1 radiance products.
    """

    def __init__(self, region, channel):
        super().__init__(16, "1b", "Rad", region, channel)


class GOES17L1BRadiances(GOESProduct):
    """
    Class representing GOES 17 L1 radiance products.
    """

    def __init__(self, region, channel):
        super().__init__(17, "1b", "Rad", region, channel)


class GOES18L1BRadiances(GOESProduct):
    """
    Class representing GOES 18 L1 radiance products.
    """

    def __init__(self, region, channel):
        super().__init__(18, "1b", "Rad", region, channel)


goes_16_l1b_radiances_c01_full_disk = GOES16L1BRadiances("F", 1)
goes_16_l1b_radiances_c02_full_disk = GOES16L1BRadiances("F", 2)
goes_16_l1b_radiances_c03_full_disk = GOES16L1BRadiances("F", 3)
goes_16_l1b_radiances_c04_full_disk = GOES16L1BRadiances("F", 4)
goes_16_l1b_radiances_rgb_full_disk = GOES16L1BRadiances("F", [1, 2, 3])
goes_16_l1b_radiances_all_full_disk = GOES16L1BRadiances("F", list(range(1, 17)))
goes_16_l1b_radiances_c01_conus = GOES16L1BRadiances("C", 1)
goes_16_l1b_radiances_c02_conus = GOES16L1BRadiances("C", 2)
goes_16_l1b_radiances_c03_conus = GOES16L1BRadiances("C", 3)
goes_16_l1b_radiances_c04_conus = GOES16L1BRadiances("C", 4)
goes_16_l1b_radiances_rgb_conus = GOES16L1BRadiances("C", [1, 2, 3])
goes_16_l1b_radiances_all_conus = GOES16L1BRadiances("C", list(range(16)))

goes_17_l1b_radiances_c01_full_disk = GOES17L1BRadiances("F", 1)
goes_17_l1b_radiances_c02_full_disk = GOES17L1BRadiances("F", 2)
goes_17_l1b_radiances_c03_full_disk = GOES17L1BRadiances("F", 3)
goes_17_l1b_radiances_c04_full_disk = GOES17L1BRadiances("F", 4)
goes_17_l1b_radiances_rgb_full_disk = GOES17L1BRadiances("F", [1, 2, 3])
goes_17_l1b_radiances_all_full_disk = GOES17L1BRadiances("F", list(range(1, 17)))
goes_17_l1b_radiances_c01_conus = GOES17L1BRadiances("C", 1)
goes_17_l1b_radiances_c02_conus = GOES17L1BRadiances("C", 2)
goes_17_l1b_radiances_c03_conus = GOES17L1BRadiances("C", 3)
goes_17_l1b_radiances_c04_conus = GOES17L1BRadiances("C", 4)
goes_17_l1b_radiances_rgb_conus = GOES17L1BRadiances("C", [1, 2, 3])
goes_17_l1b_radiances_all_conus = GOES17L1BRadiances("C", list(range(1, 17)))

goes_18_l1b_radiances_c01_full_disk = GOES18L1BRadiances("F", 1)
goes_18_l1b_radiances_c02_full_disk = GOES18L1BRadiances("F", 2)
goes_18_l1b_radiances_c03_full_disk = GOES18L1BRadiances("F", 3)
goes_18_l1b_radiances_c04_full_disk = GOES18L1BRadiances("F", 4)
goes_18_l1b_radiances_rgb_full_disk = GOES18L1BRadiances("F", [1, 2, 3])
goes_18_l1b_radiances_all_full_disk = GOES18L1BRadiances("F", list(range(1, 17)))
goes_18_l1b_radiances_c01_conus = GOES18L1BRadiances("C", 1)
goes_18_l1b_radiances_c02_conus = GOES18L1BRadiances("C", 2)
goes_18_l1b_radiances_c03_conus = GOES18L1BRadiances("C", 3)
goes_18_l1b_radiances_c04_conus = GOES18L1BRadiances("C", 4)
goes_18_l1b_radiances_rgb_conus = GOES18L1BRadiances("C", [1, 2, 3])
goes_18_l1b_radiances_all_conus = GOES18L1BRadiances("C", list(range(1, 17)))
