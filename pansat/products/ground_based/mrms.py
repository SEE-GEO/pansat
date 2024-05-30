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
from pansat.products import Product, FilenameRegexpMixin
from pansat.exceptions import NoAvailableProvider, MissingDependency
from pansat.time import TimeRange, to_datetime64
from pansat.geometry import LonLatRect


PRODUCT_NAMES = {
    "precip_rate": "PrecipRate",
    "radar_quality_index": "RadarQualityIndex",
    "precip_flag": "PrecipFlag",
    "precip_1h": "RadarOnly_QPE_01H",
    "precip_1h_gc": "GaugeCorr_QPE_01H",
    "precip_1h_ms": "MultiSensor_QPE_01H_Pass2",
}


MRMS_DOMAIN = LonLatRect(-130, 20, -60, 55)


class MRMSProduct(FilenameRegexpMixin, Product):
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
        Product.__init__(self)

    @property
    def default_destination(self):
        """Stores MRMS files in a folder called MRMS."""
        return Path("mrms")

    @property
    def name(self):
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return ".".join([prefix, "mrms", self._name])

    def filename_to_date(self, filename):
        """
        Extract data corresponding to MRMS file.
        """
        name = Path(filename).name.split(".")[1]
        return datetime.strptime(name, "00_%Y%m%d-%H%M%S")

    def get_temporal_coverage(self, rec: FileRecord):
        if isinstance(rec, (str, Path)):
            rec = FileRecord(Path(rec))

        start_time = self.filename_to_date(rec.filename)
        ttype = self.temporal_resolution.dtype
        if self.temporal_resolution > np.timedelta64(30, "m"):
            start_time = to_datetime64(start_time) - self.temporal_resolution
        else:
            start_time = to_datetime64(start_time) - 0.5 * self.temporal_resolution
        end_time = start_time + self.temporal_resolution
        return TimeRange(start_time, end_time)

    def get_spatial_coverage(self, rec: FileRecord):
        return MRMS_DOMAIN

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

    def open(self, rec):
        """
        Open a grib2 file containing MRMS precipitation rates using xarray.

        Args:
             rec: A string, pathlib.Path of FileRecord pointing to the local
                 file to open.

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
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec, product=self)

        path = rec.local_path
        if path.suffix == ".gz":
            try:
                temp = Path(mkdtemp())
                with open(path, "rb") as source:
                    bs = gzip.decompress(source.read())
                    dest = temp / "temp.grib2"
                    with open(dest, "wb") as dest_file:
                        dest_file.write(bs)
                    dataset = xr.load_dataset(dest, engine="cfgrib")
            finally:
                rmtree(temp)
        else:
            dataset = xr.load_dataset(path, engine="cfgrib")

        lons = dataset.longitude.data
        lons[lons > 180] -= 360.0

        dataset = dataset.rename(
            {
                "unknown": self.variable_name,
            }
        )
        return dataset


precip_rate = MRMSProduct("precip_rate", "precip_rate", np.timedelta64(120, "s"))
radar_quality_index = MRMSProduct(
    "radar_quality_index", "radar_quality_index", np.timedelta64(120, "s")
)
precip_flag = MRMSProduct("precip_flag", "precip_flag", np.timedelta64(120, "s"))
precip_1h = MRMSProduct("precip_1h", "precip_1h", np.timedelta64(120, "s"))
precip_1h_gc = MRMSProduct("precip_1h_gc", "precip_1h_gc", np.timedelta64(60 * 60, "s"))
precip_1h_ms = MRMSProduct("precip_1h_ms", "precip_1h_ms", np.timedelta64(60 * 60, "s"))

######################################################################
# Utility functions
######################################################################

PRECIP_TYPES = {
    "No rain": [0],
    "Stratiform, warm": [1.0, 2.0],
    "Stratiform, cool": [10.0],
    "Snow": [3.0, 4.0],
    "Convective": [6.0],
    "Hail": [7.0],
    "Tropical/stratiform mix": [91.0],
    "Tropical/convective rain mix": [96.0],
}


def extract_precip_class_map(mrms_precip_flag):
    """
    Convert MRMS precipitation classification into a class map with
    continuous integer values from 0 - 8.
    """
    result = np.nan * np.zeros_like(mrms_precip_flag)
    for ind, vals in enumerate(PRECIP_TYPES.values()):
        mask = np.zeros_like(result, dtype=bool)
        for val in vals:
            mask += np.isclose(mrms_precip_flag, val)
        result[mask] = ind
    return result
