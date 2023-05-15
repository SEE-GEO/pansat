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

import numpy as np

from pansat.products.product_description import ProductDescription
import pansat.download.providers as providers
from pansat.products.product import Product
from pansat.exceptions import NoAvailableProvider


class CloudSatProduct(Product):
    """
    The CloudSat class defines a generic interface for CloudSat products.

    Attributes:
        name(``str``): The name of the product
        description(``list``): List of variable names provided by this
            product.
    """

    def __init__(self, product_name, level, version, description):
        self.product_name = product_name
        self.level = level
        self.version = version
        self._description = description
        name = level.upper() + "-" + product_name.upper()
        self.filename_regexp = re.compile(
            r"([\d]*)_([\d]*)_CS_" + name + r"_GRANULE_P_R([\d]*)_E([\d]*)\.*"
        )

    @property
    def description(self):
        # Product description object describing the CloudSat product.
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
            filename(``str``): Filename of a CloudSat product.

        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """
        filename = os.path.basename(filename)
        filename = filename.split("_")[0]
        return datetime.strptime(filename, "%Y%j%H%M%S")

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
        ``CloudSat/<product_name>``>
        """
        return Path("CloudSat") / Path(self.name)

    @property
    def name(self):
        return f"CloudSat_{self.level}-{self.product_name}"

    def __str__(self):
        """The full product name."""
        return f"CloudSat_{self.level}-{self.product_name}"

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
        from pansat.formats.hdf4 import HDF4File

        file_handle = HDF4File(filename)
        return self.description.to_xarray_dataset(file_handle, globals())


def _cloud_scenario_to_cloud_scenario_flag(cloud_scenario):
    """
    Extract cloud class from CloudSat cloud scenario data.

    Extract bits 0 from the combined integer values, which encode the
    whether the cloud type could be determined.
    """
    cloud_scenario[:].astype(np.int16)
    mask = 0x0001
    return np.bitwise_and(cloud_scenario[:], mask)


def _cloud_scenario_to_cloud_class(cloud_scenario):
    """
    Extract cloud class from CloudSat cloud scenario data.

    Extract bits 1-4 from the combined integer values, which encode the
    cloud class.
    """
    cloud_scenario[:].astype(np.int16)
    mask = 0x001E
    return np.right_shift(np.bitwise_and(cloud_scenario[:], mask), 1)


def _cloud_scenario_to_land_sea_flag(cloud_scenario):
    """
    Extract sea flag from CloudSat cloud scenario data.

    Extract bits 5-6 from the combined integer values, which encode the
    land-sea flag.
    """
    mask = 0x0060
    return np.right_shift(np.bitwise_and(cloud_scenario[:], mask), 5)


def _cloud_scenario_to_latitude_flag(cloud_scenario):
    """
    Extract latitude flag from CloudSat cloud scenario data.

    Extract bits 7-8 from the combined integer values, which encode the
    latitude flag.
    """
    mask = 0x0180
    return np.right_shift(np.bitwise_and(cloud_scenario[:], mask), 7)


def _cloud_scenario_to_algorithm_flag(cloud_scenario):
    """
    Extract algorithm flag from CloudSat cloud scenario data.

    Extract bits 9-10 from the combined integer values, which encode the
    algorithm used for classification.
    """
    mask = 0x0600
    return np.right_shift(np.bitwise_and(cloud_scenario[:], mask), 9)


def _cloud_scenario_to_quality_flag(cloud_scenario):
    """
    Extract quality flag from CloudSat cloud scenario data.

    Extract bits 11-12 from the combined integer values, which encode the
    data quality.
    """
    mask = 0x1800
    return np.right_shift(np.bitwise_and(cloud_scenario[:], mask), 11)


def _cloud_scenario_to_precipitation_flag(cloud_scenario):
    """
    Extract precipitation flag from CloudSat cloud scenario data.

    Extract bits 13-14 from the combined integer values, which encode the
    precipitation flag.
    """
    mask = 0x6000
    return np.right_shift(np.bitwise_and(cloud_scenario[:], mask), 13)


def _parse_products():
    module_path = Path(__file__).parent
    for filename in module_path.iterdir():
        if filename.match("*.ini"):
            description = ProductDescription(filename)
            python_name = filename.name.split(".")[0]
            product_name = description.name
            level = description.properties["level"]
            version = description.properties["version"]
            globals()[python_name] = CloudSatProduct(
                product_name, level, version, description
            )


_parse_products()
