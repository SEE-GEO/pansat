"""
pansat.products.satellite.cloud_sat
===================================

This module defines the CloudSat product class, which represents all
supported CloudSat products.
"""

import re
import os
from datetime import datetime
from pathlib import Path
import pansat.download.providers as providers
from pansat.products.product import Product

class NoAvailableProviderError(Exception):
    """
    Exception indicating that no suitable provider could be found for
    a product.
    """

class CloudSatProduct(Product):
    """
    The CloudSat class defines a generic interface for CloudSat products.

    Attributes:
        name(``str``): The name of the product
        variables(``list``): List of variable names provided by this
            product.
    """
    def __init__(self, name, variables):
        self.name = name
        self._variables = variables
        self.filename_regexp = re.compile(r"([\d]*)_([\d]*)_CS_"
                                          + name +
                                          r"_GRANULE_P_R([\d]*)_E([\d]*)\.*")

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
        filename = filename.split("_")[0]
        return datetime.strptime(filename, "%Y%j%H%M%S")

    def _get_provider(self):
        """ Find a provider that provides the product. """
        available_providers = [p for p in providers.all_providers
                               if str(self) in p.get_available_products()]
        if not available_providers:
            raise NoAvailableProviderError(f"Could not find provider for the"
                                           f"the product {self.name}.")
        return available_providers[0]

    @property
    def default_destination(self):
        """
        The default destination for CloudSat product is
        ``CloudSat/<product_name>``>
        """
        return Path("CloudSat") / Path(self.name)

    def __str__(self):
        """ The full product name. """
        return "CloudSat_" + self.name

    def download(self,
                 t0,
                 t1,
                 destination = None,
                 provider = None):
        """
        Download data product for given time range.

        Args:
            start_time(``datetime``): ``datetime`` object defining the start date
                 of the time range.
            end_time(``datetime``): ``datetime`` object defining the end date of the
                 of the time range.
            destination(``str`` or ``pathlib.Path``): The destination where to store
                 the output data.
        """

        if not provider:
            provider = self._get_provider()

        if not destination:
            destination = self.default_destination
        else:
            destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        provider = provider(self)

        files = provider.get_files_in_range(t0, t1)
        print(files)
        for f in files:
            provider.download(f, destination / f)

l1b_cpr = CloudSatProduct("1B-CPR", [])
