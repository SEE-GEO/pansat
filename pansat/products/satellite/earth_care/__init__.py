"""
pansat.products.satellite.earth_care
====================================

Products from the EartCARE mission.
"""
from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Dict, List, Optional

import numpy as np
import xarray as xr

import pansat
from pansat.formats.hdf5 import HDF5File
from pansat.products.product_description import ProductDescription
from pansat.file_record import FileRecord
from pansat.granule import Granule
from pansat.products import GranuleProduct, FilenameRegexpMixin
from pansat import geometry
from pansat.time import TimeRange


class EarthCAREProduct(FilenameRegexpMixin, GranuleProduct):
    """
    Interface for EarthCARE products.
    """
    def __init__(
            self,
            level: str,
            product_name: str,
            description: ProductDescription
    ):
        """
        Args:
            level: The processing level of the product, i.e., 'l1b', 'l2b', etc.
            product_name: The name of the product, e.g., 'geoprof'
            description: The product description defining the variables to be loaded.
        """
        self.product_name = product_name
        self.level = level
        self.description = description
        name = level.upper() + "-" + product_name.upper()
        self.filename_regexp = re.compile(
            rf"ECA_EXBA_{self.product_name}_{self.level.upper()}_(\d{{8}}T\d{{6}}Z)"
            rf"_(\d{{8}}T\d{{6}}Z)_\w+.h5"
        )
        super().__init__()


    @property
    def default_destination(self):
        """
        Defauult destination used to store pansat products.
        """
        dirname = f"{self.level.lower()}_{self.product_name.lower()}"
        return Path("earth_care") / dirname

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


    def open(self, rec: FileRecord, slcs: Optional[Dict[str, slice]] = None) -> xr.Dataset:
        """
        Open file as xarray dataset.

        Args:
            rec: A file record pointing whose 'local_path' attribute points to the file to
                open.

        Return:
            An xarray.Dataset containing the data loaded from the given file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)
        file_handle = HDF5File(rec.local_path)
        dataset = self.description.to_xarray_dataset(file_handle, globals())
        if slcs is None:
            return dataset
        return dataset[slcs]

    def get_spatial_coverage(self, rec: FileRecord) -> geometry.Geometry:
        """
        Implements interface to extract spatial coverage of file.
        """
        if rec.local_path is None:
            raise ValueError(
                "This products reuqires a local file is to determine "
                " the spatial coverage."
            )

        file_handle = HDF5File(rec.local_path)
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
        fmt = "%Y%m%dT%H%M%SZ"
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
        if not isinstance(rec, FileRecord):
            rec = FileRecord(rec)
        granules = []
        file_handle = HDF5File(rec.local_path)
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
        filename = granule.file_record.local_path
        file_handle = HDF5File(filename)
        return self.description.to_xarray_dataset(
                file_handle, context=globals(), slcs=granule.get_slices())


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
    start_time = np.datetime64("2000-01-01T00:00:00")
    profile_time = handle[slcs]
    return start_time + profile_time.astype("timedelta64[s]")


def _mask_invalid(handle, slcs) -> np.ndarray:
    """
    Mask values exceeding 1e30.
    """
    data = handle[slcs]
    data[1e30 < data] = np.nan
    return data


def _parse_products():
    module_path = Path(__file__).parent
    for filename in module_path.iterdir():
        if filename.match("*.ini"):
            description = ProductDescription(filename)
            python_name = filename.name.split(".")[0]
            product_name = description.properties["name"]
            level = description.properties["level"]
            globals()[python_name] = EarthCAREProduct(
                level, product_name, description
            )

_parse_products()
