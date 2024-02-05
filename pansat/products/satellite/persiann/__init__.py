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


DTYPES = {
    "ccs": ">i2",
    "cdr": "f4"
}


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

        return TimeRange(date - self.temporal_resolution, date)


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


    def open(self, rec: FileRecord) -> xr.Dataset:
        """
        Open file as 'xarray.Dataset'.

        Args:
            rec: A FileRecord pointing to a local PERSIANN file.

        Return:
            An 'xarray.Dataset' containing the data from the given
            file.
        """
        if not isinstance(rec, FileRecord):
            rec = FileRecord(rec)

        bytes = gzip.open(rec.local_path).read()

        dtype = DTYPES[self._name[:3]]
        data = np.frombuffer(bytes, dtype)
        n_pixels = data.size
        n_rows = int(np.sqrt(n_pixels // 3))
        shape = (n_rows, 3 * n_rows)
        data = data.reshape(shape)
        data = np.concatenate(
            [data[..., shape[1] // 2:], data[..., :shape[1] // 2]],
            axis=-1
        )

        lats = np.linspace(60.0, -60.0, shape[0] + 1)
        lats = 0.5 * (lats[1:] + lats[:-1])
        lons = np.linspace(-180, 180, shape[1] + 1)
        lons = 0.5 * (lons[1:] + lons[:-1])

        time_range = self.get_temporal_coverage(rec)

        if self._name.startswith("ccs"):
            data = data / 100
        data[data < 0] = np.nan

        dataset = xr.Dataset(
            {
                "time": (("time",), [time_range.start]),
                "latitude": (("latitude",), lats),
                "longitude": (("longitude",), lons),
                "precipitation": (("time", "latitude", "longitude"), data[None]),
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
