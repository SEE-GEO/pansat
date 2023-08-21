"""
=================================
pansat.products.ground_based.mrms
=================================

This module provides product class for NOAA NSSL Multi-Radar/Multi-Sensor
system (`MRMS`_).

.. _MRMS: https://www.nssl.noaa.gov/projects/mrms/

.. note::

    MRMS files come in grib2 format and therefore requires the cfgrib package
    so that they can be read using xarray.
"""
import re
from datetime import datetime
import gzip
from pathlib import Path
from shutil import rmtree
from tempfile import mkdtemp

import xarray as xr

from pansat.download import providers
from pansat.products.product import Product
from pansat.exceptions import NoAvailableProvider, MissingDependency


class MRMSProduct(Product):
    """
    This class represents MRMS products.
    """

    def __init__(self, name, variable_name):
        """
        Create new MRMS product.

        Args:
            name: The name of the MRMS product as it appears in the file signature.
            variable_name: The name to use for the data variable when opened as
                  xarray Dataset.
        """
        self.name = f"MRMS_{name}"
        self.variable_name = variable_name
        self.filename_regexp = re.compile(name + r"_00\.00_\d{8}-\d{6}.grib2\.?g?z?")

    @property
    def default_destination(self):
        """Stores MRMS files in a folder called MRMS."""
        return Path("MRMS")

    def filename_to_date(self, filename):
        """
        Extract data corresponding to MRMS file.
        """
        name = Path(filename).name.split(".")[1]
        return datetime.strptime(name, "00_%Y%m%d-%H%M%S")

    def __str__(self):
        return self.name

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
        Open a grib2 file containing MRMS precipitation rates using xarray.

        Args:
             filename: The path to the file.

        Return:
             An xarray dataset containing the data in the file.
        """
        try:
            import cfgrib
        except ImportError as e:
            raise MissingDependency(
                f"""
                Opening grib2 files with xarray requires the cfgrib package to be installed
                and working but the following error was encountered when trying to import
                the package:

                {e}
                """
            )

        filename = Path(filename)
        if filename.suffix == ".gz":
            try:
                temp = Path(mkdtemp())
                with open(filename, "rb") as source:
                    bs = gzip.decompress(source.read())
                    dest = temp / "temp.grib2"
                    with open(dest, "wb") as dest_file:
                        dest_file.write(bs)
                    dataset = xr.load_dataset(dest, engine="cfgrib")
            finally:
                rmtree(temp)
        else:
            dataset = xr.load_dataset(filename, engine="cfgrib")
        return dataset.rename({"unknown": self.variable_name})


mrms_precip_rate = MRMSProduct("PrecipRate", "precip_rate")
mrms_radar_quality_index = MRMSProduct("RadarQualityIndex", "radar_quality_index")
mrms_precip_flag = MRMSProduct("PrecipFlag", "precip_flag")
mrms_reflectivity = MRMSProduct("SeamlessHSR", "reflectivity")
