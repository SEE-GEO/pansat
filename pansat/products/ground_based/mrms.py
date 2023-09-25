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

import numpy as np
import xarray as xr

import pansat
from pansat.download import providers
from pansat.file_record import FileRecord
from pansat.products import Product
from pansat.exceptions import NoAvailableProvider, MissingDependency
from pansat.time import TimeRange, to_datetime64


PRODUCT_NAMES = {
    "precip_rate": "PrecipRate",
    "radar_quality_index": "RadarQualityIndex",
    "precip_flag": "PrecipFlag",
}


class MRMSProduct(Product):
    """
    This class represents MRMS products.
    """

    def __init__(
        self, name: str, variable_name: str, temporal_resolution: np.timedelta64
    ):
        """
        Create new MRMS product.

        Args:
            product_name: The name of the product used within pansat.
            mrms_name: The name of the product
            name: The name of the MRMS product as it appears in the file signature.
            variable_name: The name to use for the data variable when opened as
                  xarray Dataset.
        """
        self._name = name
        self.variable_name = variable_name
        mrms_name = PRODUCT_NAMES[self._name]
        self.filename_regexp = re.compile(
            mrms_name + r"_00\.00_\d{8}-\d{6}.grib2\.?g?z?"
        )
        self.temporal_resolution = temporal_resolution

    @property
    def default_destination(self):
        """Stores MRMS files in a folder called MRMS."""
        return Path("MRMS")

    @property
    def name(self):
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return ".".join([prefix, "mrms", self._name])

    def matches(self, path):
        return self.filename_regexp(path.filename) is not None

    def filename_to_date(self, filename):
        """
        Extract data corresponding to MRMS file.
        """
        name = Path(filename).name.split(".")[1]
        return datetime.strptime(name, "00_%Y%m%d-%H%M%S")

    def get_temporal_coverage(self, rec: FileRecord):
        start_time = self.filename_to_date(rec.filename)
        end_time = to_datetime64(start_time) + self.temporal_resolution
        return TimeRange(start_time, end_time)

    def get_spatial_coverage(self, rec: FileRecord):
        return LonLatRect(-130, 20, -60, 55)

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

        lons = dataset.longitude.data
        lons[lons > 180] = lons - 360

        return dataset.rename({"unknown": self.variable_name})


precip_rate = MRMSProduct("precip_rate", "precip_rate", np.timedelta64(120, "s"))
radar_quality_index = MRMSProduct(
    "radar_quality_index", "radar_quality_index", np.timedelta64(120, "s")
)
precip_flag = MRMSProduct("precip_flag", "precip_flag", np.timedelta64(120, "s"))
