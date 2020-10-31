"""
pansat.products.reanalysis.era5
===================================
This module defines the ERA5 product class, which represents all
supported ERA5 products.


"""


import xarray
import re
import os
from datetime import datetime, timedelta
from pathlib import Path
import pansat.download.providers as providers
from pansat.products.product import Product


class NoAvailableProviderError(Exception):
    """
    Exception indicating that no suitable provider could be found for
    a product.
    """


class ERA5Product(Product):
    """
    The ERA5 class defines a generic interface for ERA5 products.


    Attributes:
        tsteps(``str``): "monthly" or "hourly" to choose resolution of output timesteps
        levels(``str``): "surface", "pressure" or "land". <surface> contains surface data
                          and column-integrated values, pressure levels contains data throughout
                          the atmosphere column and <land> contains data from surface to soil depth
        name(``str``): The full name of the product according to Copernicus webpage
        variables(``list``): List of variable names provided by this
            product.
        domain(``list``): list of strings to select region  [lat1, lat2, lon1, lon2]
                          if None: global data will be downloaded
    """

    def __init__(self, tsteps, levels, variables, domain=None, name=None):
        self.variables = variables

        if not domain:
            self.domain = "global"
        else:
            if domain[0] < -90 or domain[0] > 90 or domain[1] > 90 or domain[1] < -90:
                raise Exception("Latitude values have to be between -180 and 180.")

            if (
                domain[2] < -180
                or domain[2] > 180
                or domain[3] > 180
                or domain[3] < -180
            ):
                raise Exception("Longitude values have to be between -90 and 90.")

            self.domain = [domain[1], domain[2], domain[0], domain[3]]

        if not name:
            if levels == "surface":
                self.levels = "single-levels"
            elif levels == "pressure":
                self.levels = "pressure-levels"
            elif levels == "land":
                self.levels = levels
            else:
                raise Exception(
                    "Supported data products are: surface, pressure and land."
                )

            if tsteps == "monthly":
                self.tsteps = "monthly-means"
                self.name = self.name = (
                    "reanalysis-era5-" + self.levels + "-" + self.tsteps
                )
            elif tsteps == "hourly":
                self.tsteps = tsteps
                self.name = "reanalysis-era5-" + self.levels

            else:
                raise Exception("tsteps has to be monthly or hourly.")
        else:
            self.name = name

        self.filename_regexp = re.compile(
            self.name + r"_[\d]*.*_" + self.variables[0] + r".*.nc"
        )

    def variables(self):
        return self._variables

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
            filename(``str``): Filename of a CloudSat product.
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
        """ Find a provider that provides the product. """
        available_providers = [
            p
            for p in providers.ALL_PROVIDERS
            if str(self) in p.get_available_products()
        ]
        if not available_providers:
            raise NoAvailableProviderError(
                f"Could not find provider for the" f" product {self.name}."
            )
        return available_providers[0]

    @property
    def default_destination(self):
        """
        The default destination for CloudSat product is
        ``CloudSat/<product_name>``>
        """
        return Path("ERA5") / Path(self.name)

    def __str__(self):
        """ The full product name. """
        return self.name

    def download(self, t0, t1, destination=None, provider=None):
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

        downloaded(``list``): ``list`` with names of all downloaded files for respective data product

        """

        if not provider:
            provider = self._get_provider()

        if not destination:
            destination = self.default_destination
        else:
            destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        provider = provider(self)

        downloaded = provider.download(t0, t1, destination)

        return downloaded

    def open(self, filename):
        """Opens a given file of ERA5 product class as xarray.

        Args:
        filename(``str``): name of the file to be opened

        Returns:

        xr(``xarray.Dataset``): xarray dataset object"""
        xr = xarray.open_dataset(filename)

        return xr
