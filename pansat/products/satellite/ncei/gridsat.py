"""
pansat.products.ncei.gridsat
============================

Products for accessing GridSat products.
"""
from datetime import datetime, timedelta
from pathlib import Path
import re

import pansat
from pansat.download import providers
from pansat.exceptions import NoAvailableProvider
from pansat.file_record import FileRecord
from pansat.geometry import LonLatRect
from pansat.products import Product, FilenameRegexpMixin
from pansat.time import TimeRange


class GridsatProduct(FilenameRegexpMixin, Product):
    """
    Class for NOAA GridSat GOES and CONUS products.
    """

    def __init__(
            self,
            variant: str,
            temporal_resolution: timedelta
    ):
        """
        Args:
            variant: The variant of the GridSat product: 'conus' or 'goes'.
            temporal_resolution: timdelta object representing the temporal
                resolution of the product.
        """
        self.variant = variant
        self.temporal_resolution = temporal_resolution
        self.filename_regexp = re.compile(
            rf"GridSat-{self.variant.upper()}\.\w*\.\d{{4}}\.\d{{2}}"
            r"\.\d{2}\.\d{4}\.\w{3}\.nc"
        )
        Product.__init__(self)

    @property
    def name(self):
        """
        The product name that uniquely identifies the product within pansat.
        """
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return prefix + f".gridsat_{self.variant}"

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Args:
            filename: The name of GridSat file.
        Return:
            ``datetime.datetime`` object of the corresponding
            time.
        """
        parts = rec.filename.split(".")
        year, month, day, hour_min = parts[2:6]
        time_stamp = datetime(
            int(year), int(month), int(day), int(hour_min[:2]), int(hour_min[2:])
        )
        start_time = time_stamp - 0.5 * self.temporal_resolution
        end_time = time_stamp + 0.5 * self.temporal_resolution
        return TimeRange(start_time, end_time)

    def get_spatial_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Args:
            filename: The name of GridSat file.
        Return:
            ``datetime.datetime`` object of the corresponding
            time.
        """
        return LonLatRect(
            -180, -60, -50, 60
        )

    @property
    def default_destination(self):
        """Default destination for downloads."""
        return "grid_sat"

    def open(self, rec: FileRecord):
        """Open given file as ``xarray.Dataset``."""
        return xr.open_dataset(rec.local_path)



class GridsatB1(GridsatProduct):
    """
    Specialized class for the GridSat CDR.
    """

    def __init__(self):
        """Create product."""
        super().__init__("b1", timedelta(hours=3))
        self.filename_regexp = re.compile(
            r"GRIDSAT-B1\.\d{4}\.\d{2}"
            r"\.\d{2}\.\d{2}\.\w*\.nc"
        )

    def get_temporal_coverage(self, rec: FileRecord):
        """
        Args:
            rec: A file record identifying a GridSat B1 file.
        Return:
            A time range object specifying the temporal coverage of the
            given file.
        """
        filename = rec.filename
        parts = filename.split(".")
        year, month, day, hour = parts[1:5]
        time_stamp = datetime(int(year), int(month), int(day), int(hour))
        start = time_stamp - timedelta(hours=1, minutes=30)
        end = time_stamp + timedelta(hours=1, minutes=30)
        return TimeRange(start, end)

    def get_spatial_coverage(self, rec: FileRecord) :
        """
        Args:
            rec: A file record identifying a GridSat B1 file.
        Return:
            A geometry object representing the spatial coverage of the
            file.
        """
        return LonLatRect(
            -180, -70, 180, 70
        )


gridsat_conus = GridsatProduct(("conus"), timedelta(minutes=15))
gridsat_goes = GridsatProduct("goes", timedelta(hours=1))
gridsat_b1 = GridsatB1()
