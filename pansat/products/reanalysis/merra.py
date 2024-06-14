"""
pansat.products.reanalysis.merra
================================

This module provides product classes for the various MERRA 2 datasets.
"""
from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import List, Optional

import xarray as xr

import pansat
from pansat.file_record import FileRecord
from pansat.products import Product, FilenameRegexpMixin
from pansat.time import TimeRange
from pansat.geometry import Geometry, LonLatRect

class MERRA2(FilenameRegexpMixin, Product):
    """
    Base class for data products from the MERRA2 reanalysis.
    """
    def __init__(
        self,
        collection: str,
        variables: Optional[List] = None
    ):
        self.collection = collection
        self.variables = variables
        super().__init__()

        self.filename_regexp = re.compile(
            rf"MERRA2_\d\d\d\.{collection[2]}\w+{collection[3]}_\w+_{collection[-3:]}_\w+\.(\d{{8}})\.nc4"
        )

    @property
    def name(self) -> str:
        """
        The name uniquely identifying the product within pansat.
        """
        module = Path(__file__)
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")[:-3]
        return ".".join([prefix, self.collection])

    @property
    def default_destination(self) -> Path:
        return Path("merra2")

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Get temporal coverage of a MERRA2 product file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec, product=self)

        match = self.filename_regexp.match(rec.filename)
        if match is None:
            raise RuntimeError(
                "The provided file record %s is not a MERRA2 product file.",
                rec
            )
        date = match.group(1)
        start_date = datetime.strptime(date, "%Y%m%d")
        end_date = start_date + timedelta(days=1)
        return TimeRange(start_date, end_date)

    def get_spatial_coverage(self, rec: FileRecord) -> Geometry:
        """
        Determine spatial coverage of product file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec, product=self)

        if rec.local_path is not None:
            with xr.open_dataset(rec.local_path) as data:
                lons = data.lon.data
                lats = data.lat.data
                lon_min, lon_max = lons.min(), lons.max()
                lat_min, lat_max = lats.min(), lats.max()
                return LonLatRect(lon_min, lat_min, lon_max, lat_max)

        return LonLatRect(-180, -90, 180, 90)

    def open(self, rec: FileRecord) -> xr.Dataset:
        """
        Load data from a MERRA2 product file into an xarray.Dataset.

        Args:
            rec: A file record pointing to the file to open.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec, product=self)

        if rec.local_path is None:
            rec = rec.get()

        return xr.load_dataset(rec.local_path)


m2i3nwasm = MERRA2("m2i3nvasm")
m2i1nxasm = MERRA2("m2i1nxasm")
m2t1nxlnd = MERRA2("m2t1nxlnd")
m2t1nxflx = MERRA2("m2t1nxflx")
m2t1nxrad = MERRA2("m2t1nxrad")


class MERRA2Constant(MERRA2):
    """
    Base class for data products from the MERRA2 reanalysis.
    """
    def __init__(
        self,
        collection: str,
        variables: Optional[List] = None
    ):
        super().__init__(collection, variables)

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Get temporal coverage of a MERRA2 product file.
        """
        return TimeRange(
            datetime(1980, 1, 1),
            datetime.now()
        )

m2conxasm = MERRA2Constant("m2conxasm")
m2conxctm = MERRA2Constant("m2conxctm")
