"""
pansat.products.ground_based.kma
================================

Provides a pansat product for KMA-provided groun-radar data.
"""
from datetime import datetime, timedelta
from functools import cache
from pathlib import Path
import re

import numpy as np
from pyresample import geometry
import xarray as xr

import pansat
from pansat import TimeRange, FileRecord
from pansat.exceptions import MissingDependency
from pansat.geometry import LonLatRect
from pansat.products import Product, FilenameRegexpMixin


LAT_MIN = 31.433016
LAT_MAX = 40.397026
LON_MAX = 132.30742
LON_MIN = 120.64956


@cache
def get_kma_coords() -> xr.Dataset:
    """
    Load Dataset containing longitude and latitude coordinates of the KMA data.
    """
    return xr.load_dataset(Path(__file__).parent / "kma_grid.nc")


class KMARadarProduct(FilenameRegexpMixin, Product):
    """
    This class represents KMA  radar products.
    """

    def __init__(self):
        """
        Create KMA radar product.
        """
        self.filename_regexp = re.compile("AWS_Interp_Resol1km_aug1_QC0_\d{12}.nc")
        Product.__init__(self)

    @property
    def default_destination(self):
        """Stores AMeDAS files in a folder called amedas."""
        return Path("aws")

    @property
    def name(self):
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return ".".join([prefix, "kma", "precip_rate"])

    def filename_to_date(self, filename):
        """
        Extract data corresponding to MRMS file.
        """
        name = Path(filename).stem.split("_")[-1]
        return datetime.strptime(name, "%Y%m%d%H%M")

    def get_temporal_coverage(self, rec: FileRecord):
        if isinstance(rec, (str, Path)):
            rec = FileRecord(Path(rec))

        start_time = self.filename_to_date(rec.filename)
        end_time = start_time + timedelta(minutes=10)
        return TimeRange(start_time, end_time)

    def get_spatial_coverage(self, rec: FileRecord):
        coords = get_kma_coords()
        lons = coords.longitude.data
        lats = coords.latitude.data
        domain = LonLatRect(lons.min(), lats.min(), lons.max(), lats.max())
        return domain

    def __str__(self):
        return self.name

    def open(self, rec):
        """
        Loads KMA data and adds coordinates.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec, product=self)

        data = xr.load_dataset(rec.local_path)
        coords = get_kma_coords()
        data["longitude"] = coords.longitude
        data["latitude"] = coords.latitude
        return data


precip_rate = KMARadarProduct()
