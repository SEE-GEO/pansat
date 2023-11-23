"""
pansat.products.satellite.persiann
==================================

This module provides and interface to download and open data from the
PERSIANN suite or precipitation retrieval algorithms.
"""
from datetime import datetime, timedelta
import gzip
from pathlib import Path
import re

import numpy as np
import xarray as xr

import pansat
import pansat.download.providers as providers
from pansat.file_record import FileRecord
from pansat.products import Product, FilenameRegexpMixin
from pansat.geometry import LonLatRect
from pansat.time import TimeRange


class PersiannProduct(FilenameRegexpMixin, Product):
    """
    Base class for PERSIANN precipitation products.
    """

    def __init__(
            self,
            name: str,
            file_prefix: str,
            temporal_resolution: timedelta
    ):
        self._name = name
        self.filename_regexp = re.compile(
            rf"{file_prefix}[\w\d]*\.bin\.gz"
        )
        self.temporal_resolution = temporal_resolution
        Product.__init__(self)

    @property
    def name(self) -> str:
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return prefix + "." + self._name

    @property
    def default_destination(self):
        return Path("persiann")

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Determine temporal coverage of a given file.

        Args:
            rec: A file record identifying a given product file.

        Return:
            A TimeRange object representing the temporal coverage of the
            product.
        """
        if not isinstance(rec, FileRecord):
            rec = FileRecord(rec)

        filename = rec.filename
        if self.temporal_resolution >= timedelta(days=365):
            year = int(filename.split(".")[0][-2:])
            if year < 80:
                year += 2_000
            else:
                year += 1_900
            date = datetime(year=year, month=1, day=1)
        elif self.temporal_resolution >= timedelta(days=30):
            date = datetime.strptime(filename.split(".")[0][-4:], "%y%m")
        elif self.temporal_resolution >= timedelta(days=1):
            date = datetime.strptime(filename.split(".")[0][-5:], "%y%j")
        else:
            date = datetime.strptime(filename.split(".")[0][-7:], "%y%j%H")

        return TimeRange(date, date + self.temporal_resolution)


    def get_spatial_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Determine spatial coverage of a given file.

        Args:
            rec: A file record identifying a given product file.

        Return:
            A geometry object representing the geographical coverage of the
            data.
        """
        return LonLatRect(-180, -60, 180, 60)


    def open(self, filename):
        """
        Open file as 'xarray.Dataset'.

        Args:
            filename: Path to the file to open.

        Return:
            An 'xarray.Dataset' containing the data from the given
            file.
        """
        bytes = gzip.open(filename).read()
        shape = (3000, 9000)

        data = np.frombuffer(bytes, ">i2").reshape(shape)
        lons = np.linspace(0.02, 359.98, 9000)
        lats = np.linspace(59.98, -59.98, 3000)

        date = self.filename_to_date(filename)

        data = data / 100
        data[data < 0] = np.nan

        dataset = xr.Dataset(
            {
                "time": (("time",), [date]),
                "latitude": (("latitude",), lats),
                "longitude": (("longitude",), lons),
                "precipitation": (("time", "latitude", "longitude"), data[np.newaxis]),
            }
        )
        return dataset


cdr_daily = PersiannProduct("cdr_daily", "aB1_", timedelta(days=1))
cdr_monthly = PersiannProduct("cdr_monthly", "aB1_", timedelta(days=30))
cdr_yearly = PersiannProduct("cdr_yearly", "aB1_", timedelta(days=365))
ccs_3h = PersiannProduct("ccs_3h", "rgccs", timedelta(hours=3))
ccs_6h = PersiannProduct("ccs_6h", "rgccs", timedelta(hours=6))
ccs_daily = PersiannProduct("ccs_daily", "rgccs", timedelta(days=1))
ccs_monthly = PersiannProduct("ccs_monthly", "rgccs", timedelta(days=30))
ccs_yearly = PersiannProduct("ccs_yearly", "rgccs", timedelta(days=365))
