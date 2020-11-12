"""
pansat.products.satellite.gpm
=============================

This module defines the GPM product class, which is used to represent all
GPM products.
"""
__file__ = "." 
import re
from datetime import datetime
from pathlib import Path

import pansat.download.providers as providers
from pansat.products.product import Product
from pansat.products.product_description import ProductDescription


class NoAvailableProviderError(Exception):
    """
    Exception indicating that no suitable provider could be found for
    a product.
    """


class GPMProduct(Product):
    """
    Base class for products from the GPM mission.
    """
    def __init__(self,
                 level,
                 platform,
                 sensor,
                 name,
                 version,
                 variant,
                 description):
        self.level = level
        self.platform = platform
        self.sensor = sensor
        self.name = name
        self.version = version
        self.variant = variant
        self._description = description
        self.filename_regexp = re.compile(
            rf"{self.level}\.{self.platform}\.{self.sensor}\.{self.name}(\w*).(\d{{8}})-"
            r"S(\d{6})-E(\d{6})\.(\w*)\.(\w*).HDF5"
        )

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
        The default destination for GPM product is
        ``GPM/<product_name>``>
        """
        return Path("GPM") / Path(self.name)

    def __str__(self):
        if self.variant:
            variant = f"_{self.variant}"
        else:
            variant = ""
        s = (f"GPM_2A{self.name}{self.platform}{self.sensor}"
             f"{variant}.{self.version:02}")
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
            filename(``pathlib.Path`` or ``str``): The GPM file to open.
        """
        from pansat.formats.hdf5 import HDF5File
        file_handle = HDF5File(filename)
        return self.description.to_xarray_dataset(file_handle, globals())

class GPROFProduct(GPMProduct):
    """
    Specialization of GPM product for GPROF products, which all have the same
    data format.
    """
    def __init__(self, platform, sensor, version, variant=""):
        module_path = Path(__file__).parent
        description = ProductDescription(module_path / "gprof.ini")
        super().__init__("2A", platform, sensor, "GPROF", version, variant, description)

l2a_gprof_gpm_gmi = GPROFProduct("GPM", "GMI", 5)
l2a_gprof_metopb_mhs = GPROFProduct("METOPB", "MHS", 5)

def _parse_products():
    module_path = Path(__file__).parent
    for filename in module_path.iterdir():
        if filename.match("*.ini") and filename.name != "gprof.ini":
            description = ProductDescription(filename)
            python_name = filename.name.split(".")[0]
            product_name = description.name
            globals()[python_name] = GPMProduct(product_name, description)


_parse_products()
