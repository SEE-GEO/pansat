"""
pansat.products.reanalysis.era5
===============================
This module defines the ERA5 product class, which represents all
supported ERA5 products.


"""
import re
import os
from datetime import datetime
from pathlib import Path

import xarray

import pansat.download.providers as providers
from pansat.products.product import Product
from pansat.exceptions import NoAvailableProvider


class ERA5Product(Product):
    """
    The ERA5 class defines a generic interface for ERA5 products.

    Attributes:
        levels(``str``): "surface", "pressure" or "land". <surface> contains surface data
                          and column-integrated values, pressure levels contains data throughout
                          the atmosphere column and <land> contains data from surface to soil depth
        name(``str``): The full name of the product according to Copernicus webpage
        variables(``list``): List of variable names provided by this
            product.
        domain(``list``): list of strings to select region  [lat1, lat2, lon1, lon2]
                          if None: global data will be downloaded
    """

    def __init__(self, levels, variables, domain=None):
        self.variables = variables

        if not domain:
            self.domain = "global"
        else:
            if domain[0] < -90 or domain[0] > 90 or domain[1] > 90 or domain[1] < -90:
                raise Exception("Latitude values have to be between -90 and 90.")

            if (
                domain[2] < -180
                or domain[2] > 180
                or domain[3] > 180
                or domain[3] < -180
            ):
                raise Exception("Longitude values have to be between -180 and 180.")

            self.domain = domain

        if levels == "surface":
            self.levels = "single-levels"
        elif levels == "pressure":
            self.levels = "pressure-levels"
        elif levels == "land":
            self.levels = levels
        else:
            raise Exception("Supported data products are: surface, pressure and land.")

        if self.tsteps == "monthly-means":
            self.name = self.name = "reanalysis-era5-" + self.levels + "-" + self.tsteps
        elif self.tsteps == "hourly":
            self.name = "reanalysis-era5-" + self.levels

        else:
            raise Exception("tsteps has to be monthly or hourly.")

        self.filename_regexp = re.compile(
            self.name + r"_[\d]*.*_" + self.variables[0] + r".*.nc"
        )

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
        filename = filename.split("_")[1]
        if "monthly" in self.name:
            pattern = "%Y%m"
        else:
            pattern = "%Y%m%d%H"
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
                f"Could not find provider for the" f" product {self.name}."
            )
        return available_providers[0]

    @property
    def default_destination(self):
        """
        The default destination for ERA5 product is
        ``ERA5/<product_name>``>
        """
        return Path("ERA5") / Path(self.name)

    def __str__(self):
        """The full product name."""
        return self.name

    def download(self, start, end, destination=None):
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


class ERA5Monthly(ERA5Product):
    """

    Child Class of ERA5Product for monthly ERA5 data.

    """

    def __init__(self, levels, variables, domain=None):
        self.tsteps = "monthly-means"
        super().__init__(levels, variables, domain)


class ERA5Hourly(ERA5Product):
    """

    Child Class of ERA5Product for hourly ERA5 data.

    """

    def __init__(self, levels, variables, domain=None):
        self.tsteps = "hourly"
        super().__init__(levels, variables, domain)
