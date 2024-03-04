"""
pansat.products.satellite.cloudsat
===================================

This module defines various CloudSat data products.
"""
from datetime import datetime, timedelta
from pathlib import Path
import os
import re
from typing import List

import numpy as np
import xarray as xr

import pansat
from pansat.file_record import FileRecord
from pansat.granule import Granule
from pansat.products import GranuleProduct, FilenameRegexpMixin
from pansat.products.product_description import ProductDescription
import pansat.download.providers as providers
from pansat.exceptions import NoAvailableProvider
from pansat import geometry
from pansat.time import TimeRange


class CloudSatProduct(FilenameRegexpMixin, GranuleProduct):
    """
    The CloudSat class defines a generic interface for CloudSat products.
    """

    def __init__(
            self,
            level: str,
            product_name: str,
            version: str,
            description: ProductDescription
    ):
        """
        Args:
            level: The processing level of the product, i.e., 'l1b', 'l2b', etc.
            product_name: The name of the product, e.g., 'geoprof'
            version: The product version.
            description: The product description defining the variables to be loaded.
        """
        self.product_name = product_name
        self.level = level
        self.version = version
        self._description = description
        name = level.upper() + "-" + product_name.upper()
        self.filename_regexp = re.compile(
            r"([\d]*)_([\d]*)_CS_" + name + r"_GRANULE_P\d*_R([\d]*)_E([\d]*)\.*"
        )

    @property
    def description(self):
        return self._description

    @property
    def default_destination(self):
        """
        Defauult destination used to store pansat products.
        """
        dirname = f"{self.level.lower()}_{self.product_name.lower()}_{self.version.lower()}"
        return Path("cloudsat") / dirname

    @property
    def name(self):
        """
        The name of the product used to identify it within pansat.
        """
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")

        pname = self.product_name.lower().replace("-", "_")
        name = f"l{self.level.lower()}_{pname}"

        return ".".join([prefix, name])

    def open(self, rec: FileRecord) -> xr.Dataset:
        """
        Open file as xarray dataset.

        Args:
            rec: A file record pointing whose 'local_path' attribute points to the file to
                open.

        Return:
            An xarray.Dataset containing the data loaded from the given file.
        """
        from pansat.formats.hdf4 import HDF4File
        if isinstance(rec, str):
            rec = FileRecord(rec)
        file_handle = HDF4File(rec.local_path)
        return self.description.to_xarray_dataset(file_handle, globals())

    def get_spatial_coverage(self, rec: FileRecord) -> geometry.Geometry:
        """
        Implements interface to extract spatial coverage of file.
        """
        if rec.local_path is None:
            raise ValueError(
                "This products reuqires a local file is to determine "
                " the spatial coverage."
            )

        file_handle = HDF4File(rec.local_path)
        lons, lats = self.description.load_lonlats(file_handle, slice(0, None, 1))
        poly = geometry.parse_swath(lons, lats, m=10, n=1)
        return geometry.ShapelyGeometry(poly)

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Implements interface to extract temporal coverage of file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        match = self.filename_regexp.match(rec.filename)
        if match is None:
            raise RuntimeError(
                f"Provided file record with filename {rec.filename} doest not "
                " match the products filename regexp "
                f"{self.filename_regexp.pattern}. "
            )
        date = match[1]
        fmt = "%Y%j%H%M%S"
        start = datetime.strptime(date, fmt)
        end = datetime.strptime(date, fmt) + timedelta(minutes=90)
        if end < start:
            end += timedelta(days=1)
        return TimeRange(start, end)

    def get_granules(self, rec: FileRecord) -> List[Granule]:
        """
        Return granules in file.

        Args:
            rec: A file record object pointing to a local CloudSat file.

        Return:
            A list of granules representing the temporal and spatial coverage
            of the data.
        """
        from pansat.formats.hdf4 import HDF4File
        if not isinstance(rec, FileRecord):
            rec = FileRecord(rec)
        granules = []
        file_handle = HDF4File(rec.local_path)
        for granule_data in self.description.get_granule_data(
                file_handle, globals() ):
            granules.append(Granule(rec, *granule_data))
        return granules

    def open_granule(self, granule: Granule) -> xr.Dataset:
        """
        Open a given granule.

        Args:
            granule: The granule to open.

        Return:
            An xarray.Dataset containing only the data from the granule.
        """
        from pansat.formats.hdf4 import HDF4File
        filename = granule.file_record.local_path
        file_handle = HDF4File(filename)
        return self.description.to_xarray_dataset(
                file_handle, context=globals(), slcs=granule.get_slices())


class CSTRACKProduct(FilenameRegexpMixin, GranuleProduct):
    """
    Product class for CSTRACK products.
    """
    def __init__(
            self,
            product_name: str,
            version: str,
            description
    ):
        """
        Args:
            product_name: The name of the CSTRACK product including the leading CS-
            description: The product description defining the variables to be loaded.
        """
        self.product_name = product_name
        self.version = version
        self._description = description
        self.filename_regexp = re.compile(
            rf"CSTRACK_({self.product_name.upper()})_(\d{{13}})_(\d*)_{self.version}.hdf"
        )

    @property
    def description(self):
        return self._description

    @property
    def default_destination(self):
        """
        Defauult destination used to store pansat products.
        """
        dirname = self.product_name.lower()
        return Path("cloudsat") / dirname

    @property
    def name(self):
        """
        The name of the product used to identify it within pansat.
        """
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")

        prod_name = self.product_name.replace("-", "_").lower()
        name = f"cstrack_{prod_name}"
        return ".".join([prefix, name])

    def open(self, rec: FileRecord) -> xr.Dataset:
        """
        Open file as xarray dataset.

        Args:
            rec: A file record pointing whose 'local_path' attribute points to the file to
                open.

        Return:
            An xarray.Dataset containing the data loaded from the given file.
        """
        from pansat.formats.hdf4 import HDF4File
        if isinstance(rec, str):
            rec = FileRecord(rec)
        file_handle = HDF4File(rec.local_path)
        return self.description.to_xarray_dataset(file_handle, globals())

    def get_spatial_coverage(self, rec: FileRecord) -> geometry.Geometry:
        """
        Implements interface to extract spatial coverage of file.
        """
        if rec.local_path is None:
            raise ValueError(
                "This products reuqires a local file is to determine "
                " the spatial coverage."
            )

        file_handle = HDF4File(rec.local_path)
        lons, lats = self.description.load_lonlats(file_handle, slice(0, None, 1))
        poly = geometry.parse_swath(lons, lats, m=10, n=1)
        return geometry.ShapelyGeometry(poly)

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Implements interface to extract temporal coverage of file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        match = self.filename_regexp.match(rec.filename)
        if match is None:
            raise RuntimeError(
                f"Provided file record with filename {rec.filename} doest not "
                " match the products filename regexp "
                f"{self.filename_regexp.pattern}. "
            )
        date = match[2]
        fmt = "%Y%j%H%M%S"
        start = datetime.strptime(date, fmt)
        end = datetime.strptime(date, fmt) + timedelta(minutes=90)
        if end < start:
            end += timedelta(days=1)
        return TimeRange(start, end)

    def get_granules(self, rec: FileRecord) -> List[Granule]:
        """
        Return granules in file.

        Args:
            rec: A file record object pointing to a local CloudSat file.

        Return:
            A list of granules representing the temporal and spatial coverage
            of the data.
        """
        from pansat.formats.hdf4 import HDF4File
        if not isinstance(rec, FileRecord):
            rec = FileRecord(rec)
        granules = []
        file_handle = HDF4File(rec.local_path)
        for granule_data in self.description.get_granule_data(
                file_handle, globals() ):
            granules.append(Granule(rec, *granule_data))
        return granules

    def open_granule(self, granule: Granule) -> xr.Dataset:
        """
        Open a given granule.

        Args:
            granule: The granule to open.

        Return:
            An xarray.Dataset containing only the data from the granule.
        """
        from pansat.formats.hdf4 import HDF4File
        filename = granule.file_record.local_path
        file_handle = HDF4File(filename)
        return self.description.to_xarray_dataset(
                file_handle, context=globals(), slcs=granule.get_slices())


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


def _calculate_profile_time(handle, slcs) -> np.ndarray:
    """
    Calculate profile time as np.datetime64 objects.

    Args:
        handle: File handle to the data file.
        slcs: Optional tuple of slices to subset the data to load.

    Return:
        A numpy.ndarray containing the profiles times represented
        in datetime64 format.
    """
    start_time = getattr(handle, "TAI_start")[:][..., 0]
    profile_time = getattr(handle, "Profile_time")[slcs][..., 0]
    start_time = np.datetime64("1993-01-01") + np.round(start_time).astype("timedelta64[s]")[0]
    offsets = np.round(profile_time).astype("timedelta64[s]")
    return (start_time + offsets).astype("datetime64[ns]")

def _squeeze(handle, slcs) -> np.ndarray:
    """
    Used trailing dimensions in latitude and longitude arrays.
    """
    return np.array(handle.__getitem__(slcs)).squeeze()


def _parse_products():
    module_path = Path(__file__).parent
    for filename in module_path.iterdir():
        if filename.match("*.ini"):
            if filename.name.startswith("cstrack"):
                description = ProductDescription(filename)
                python_name = filename.name.split(".")[0]
                product_name = description.properties["product_name"]
                version = description.properties["version"]
                globals()[python_name] = CSTRACKProduct(
                    product_name, version, description
                )
            else:
                description = ProductDescription(filename)
                python_name = filename.name.split(".")[0]
                product_name = description.name
                level = description.properties["level"]
                version = description.properties["version"]
                globals()[python_name] = CloudSatProduct(
                    level, product_name, version, description
                )


_parse_products()
