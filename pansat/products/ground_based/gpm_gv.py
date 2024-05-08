"""
======================
pansat.products.gpm_gv
======================

This module provides product class for the GPM ground validation products.
"""
import re
from datetime import datetime, timedelta
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
from pansat.products.ground_based.mrms import MRMS_DOMAIN


class NMQProduct(FilenameRegexpMixin, Product):
    """
    Ground-radar GPM ground validation product.
    """

    def __init__(
            self, name: str, platform: str, dtype: np.dtype, temporal_resolution: timedelta
    ):
        """
        Create new NMQ product.

        Args:
            name: The name of the product. Should be one of '1hcf', 'precip_rate', 'mask',
                'rqi'
            platform: The satellite platform for which the ground-validation data is derived.
        """
        self._name = name
        self.platform = platform
        self.dtype = dtype
        self.temporal_resolution = temporal_resolution
        self.filename_regexp = re.compile(
            rf"{name.upper().replace('_', '')}(\.GC)?\.(\d{{8}})\.(\d{{6}})\.(\d*)\.\w\w\w\.gz"
        )
        Product.__init__(self)

    @property
    def default_destination(self):
        """Stores MRMS files in a folder called MRMS."""
        return Path("gpm_gv")

    @property
    def name(self):
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        full_name = f"{self._name}_{self.platform}"
        return ".".join([prefix, "gpm_gv", full_name])

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Determine temporal coverage of a given data file.

        Args:
            rec: A file record object pointing to a data file.

        Return:
            A time range object representing the temporal validity range of the
            given data file.

        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(Path(rec))

        match = self.filename_regexp.match(rec.filename)
        yearmonthday = match.group(2)
        hourminutesecond = match.group(3)
        time = datetime.strptime("".join((yearmonthday, hourminutesecond)), "%Y%m%d%H%M%S")
        if self.temporal_resolution > np.timedelta64(30, "m"):
            start = time -  self.temporal_resolution
            end = time
        else:
            start = time - 0.5 * self.temporal_resolution
            end = time - 0.5 * self.temporal_resolution
        return TimeRange(start, end)

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

    def open(self, rec: FileRecord) -> xr.Dataset:
        """
        Open GPM GV ASCII file.

        Args:
             rec: A string, pathlib.Path of FileRecord pointing to the local
                 file to open.

        Return:
             An xarray dataset containing the data in the file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        local_path = rec.local_path
        header = np.loadtxt(local_path, usecols=(1,), max_rows=6)
        n_cols = int(header[0])
        n_rows = int(header[1])
        lon_ll = float(header[2])
        lat_ll = float(header[3])
        dl = float(header[4])

        lons = lon_ll + np.arange(n_cols) * dl
        lats = (lat_ll + np.arange(n_rows) * dl)[::-1]

        time_range = self.get_temporal_coverage(rec)
        time = time_range.start + 0.5 * (time_range.end - time_range.start)

        data = np.zeros((n_rows, n_cols))
        data[:, :] = np.loadtxt(local_path, skiprows=6, dtype=self.dtype)

        dims = ("latitude", "longitude")
        dataset = xr.Dataset({
            "latitude": (("latitude",), lats),
            "longitude": (("longitude",), lons),
            "time": time,
            self._name: (("latitude", "longitude"), data),
        })
        return dataset


precip_rate_gpm = NMQProduct("precip_rate", "gpm", np.float32, timedelta(minutes=2))
mask_gpm = NMQProduct("mask", "gpm", np.float32, timedelta(minutes=2))
rqi_gpm = NMQProduct("rqi", "gpm", np.float32, timedelta(minutes=2))
gcf_gpm = NMQProduct("1hcf", "gpm", np.float32, timedelta(hours=1))
