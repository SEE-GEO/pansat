"""
pansat.products.ground_based.stage4
===================================

Provides a pansat product for StageIV radar data.
"""
from calendar import monthrange
from datetime import datetime, timedelta
from functools import cache
from pathlib import Path
import re
import tempfile
from typing import Optional, Dict

import numpy as np
import xarray as xr

import pansat
from pansat import TimeRange, FileRecord
from pansat.exceptions import MissingDependency
from pansat.geometry import LonLatRect
from pansat.products import Product, FilenameRegexpMixin


LAT_MIN = 19.7950190
LAT_MAX = 57.8599970
LON_MAX = -134.04273913
LON_MIN = -59.91612484
import tarfile
import tempfile
import shutil
import os
import subprocess
import xarray as xr
from pathlib import Path


def load_stage4_monthly_tar(tar_path: Path) -> xr.Dataset:
    """
    Load a full month of Stage IV data from a .tar archive into an xarray.Dataset.

    Args:
        tar_path (str or Path): Path to stage4.YYYYMM.tar file.

    Returns:
        xarray.Dataset: Dataset with time dimension containing hourly precipitation.
    """
    tar_path = Path(tar_path)
    datasets = []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Extract main monthly tar
        with tarfile.open(tar_path) as month_tar:
            month_tar.extractall(path=tmpdir)

        # Loop over daily ST4.YYYYMMDD tar files
        for daily_tar in sorted(tmpdir.glob("ST4.*")):
            if not tarfile.is_tarfile(daily_tar):
                continue
            with tarfile.open(daily_tar) as day_tar:
                day_tar.extractall(path=tmpdir)

        # Loop over all hourly .Z files
        for z_file in sorted(tmpdir.glob("ST4.*.01h.Z")):
            # Decompress .Z file (system uncompress required)
            grib_file = z_file.with_suffix("")  # remove .Z extension
            subprocess.run(["uncompress", str(z_file)], check=True)

            # Try loading the decompressed GRIB file
            try:
                ds = xr.load_dataset(
                    grib_file,
                    engine="cfgrib",
                    backend_kwargs={"indexpath": ""}
                )
                ds = ds.rename(tp="surface_precip")
                ds.surface_precip = ds.surface_precip.astype(np.float32)
                # Add timestamp from filename
                timestamp = grib_file.name.split(".")[1]  # ST4.YYYYMMDDHH
                ds = ds.expand_dims(time=[timestamp])
                datasets.append(ds)
            except Exception as e:
                print(f"Skipping {grib_file.name}: {e}")

        # Combine all datasets along time
        if datasets:
            combined = xr.concat(datasets, dim="time")
            return combined
        else:
            raise RuntimeError("No valid GRIB messages found in archive.")



class Stage4RadarProduct(FilenameRegexpMixin, Product):
    """
    This class represents Stage IV precipitation products.
    """

    def __init__(self):
        """
        Create Stage IV radar product.
        """
        self.filename_regexp = re.compile("stage4\.\d\d\d\d\d\d\.tar")
        Product.__init__(self)

    @property
    def default_destination(self):
        """Stores Stage IV files in a folder called amedas."""
        return Path("stage4")

    @property
    def name(self):
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return ".".join([prefix, "stage4", "surface_precip"])

    def filename_to_date(self, filename):
        """
        Extract data corresponding to Stage IV file.
        """
        yearmonth = Path(filename).stem.split(".")[1]
        return datetime.strptime(yearmonth, "%Y%m")

    def get_temporal_coverage(self, rec: FileRecord):
        if isinstance(rec, (str, Path)):
            rec = FileRecord(Path(rec))
        start_time = self.filename_to_date(rec.filename)
        _, n_days = monthrange(start_time.year, start_time.month)
        end_time = start_time + timedelta(days=n_days)
        return TimeRange(start_time, end_time)

    def get_spatial_coverage(self, rec: FileRecord):
        return LonLatRect(LON_MIN, LAT_MIN, LON_MAX, LAT_MAX)

    def __str__(self):
        return self.name

    def open(self, rec, slcs: Optional[Dict[str, slice]] = None):
        """
        Loads StageIV data.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec, product=self)

        data = load_stage4_monthly_tar(rec.local_path)
        return data


surface_precip = Stage4RadarProduct()
