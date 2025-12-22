"""
pansat.products.reanalysis.geos
================================

This module provides product classes for NASA's Goddard Earth Observing System (GEOS) model data.
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


class GEOSAnalysisProduct(FilenameRegexpMixin, Product):
    """
    Product class representing analysis data products from the Goddard Earth Observing System
    (GEOS) forecasting system.
    """

    def __init__(
        self,
        collection: str,
    ):
        """
        Args:
            collection: A string specifying the collection name
        """
        self.collection = collection
        super().__init__()

        self.filename_regexp = re.compile(
            rf"GEOS\.fp\.asm\.{collection}.(\d{{8}})_(\d{{4}})\.V\d+\.nc4"
        )

    @property
    def name(self) -> str:
        """
        The name uniquely identifying the product within pansat.
        """
        module = Path(__file__)
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")[:-3]
        return ".".join([prefix, self.collection.lower()])

    @property
    def default_destination(self) -> Path:
        return Path("geos")

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Get temporal coverage of a GEOS product file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec, product=self)

        match = self.filename_regexp.match(rec.filename)
        if match is None:
            raise RuntimeError(
                "The provided file record %s is not a GEOS product file.", rec
            )
        date_1 = match.group(1)
        date_2 = match.group(2)
        start_date = datetime.strptime(date_1 + date_2, "%Y%m%d%H%M")
        end_date = start_date + timedelta(hours=int(self.collection[4]))
        return TimeRange(start_date, end_date)

    def get_spatial_coverage(self, rec: FileRecord) -> Geometry:
        """
        Determine spatial coverage of product file.
        """
        return LonLatRect(-180, -90, 180, 90)

    def open(self, rec: FileRecord) -> xr.Dataset:
        """
        Load data from a GEOS product file into an xarray.Dataset.

        Args:
            rec: A file record pointing to the file to open.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec, product=self)

        if rec.local_path is None:
            rec = rec.get()

        return xr.load_dataset(rec.local_path)


inst3_3d_asm_nv = GEOSAnalysisProduct("inst3_3d_asm_Nv")
inst3_2d_asm_nx = GEOSAnalysisProduct("inst3_2d_asm_Nx")
tavg1_2d_lnd_nx = GEOSAnalysisProduct("tavg1_2d_lnd_Nx")
tavg1_2d_flx_nx = GEOSAnalysisProduct("tavg1_2d_flx_Nx")
tavg1_2d_rad_nx = GEOSAnalysisProduct("tavg1_2d_rad_Nx")
i3nvasm = GEOSAnalysisProduct("inst3_2d_asm_Nx")


class GEOSForecastProduct(FilenameRegexpMixin, Product):
    """
    Product class representing forecast data products from the Goddard Earth Observing System (GEOS)
    forecasting system.
    """

    def __init__(
        self,
        collection: str,
    ):
        """
        Args:
            collection: A string specifying the collection name
        """
        self.collection = collection
        super().__init__()

        self.filename_regexp = re.compile(
            rf"GEOS\.fp\.fcst\.{collection}.(\d{{8}})_(\d{{2}})(\+)(\d{{8}})_(\d{{4}})\.V\d+\.nc4"
        )

    @property
    def name(self) -> str:
        """
        The name uniquely identifying the product within pansat.
        """
        module = Path(__file__)
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")[:-3]
        return ".".join([prefix, self.collection.lower() + "_fc"])

    @property
    def default_destination(self) -> Path:
        return Path("geos_forecast")

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Get temporal coverage of a GEOS product file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec, product=self)

        match = self.filename_regexp.match(rec.filename)
        if match is None:
            raise RuntimeError(
                "The provided file record %s is not a GEOS product file.", rec
            )
        date_1 = match.group(1)
        date_2 = match.group(2)
        start_date = datetime.strptime(date_1 + date_2, "%Y%m%d%H")
        end_date = start_date + timedelta(hours=6)
        return TimeRange(start_date, end_date)

    def get_spatial_coverage(self, rec: FileRecord) -> Geometry:
        """
        Determine spatial coverage of product file.
        """
        return LonLatRect(-180, -90, 180, 90)

    def open(self, rec: FileRecord) -> xr.Dataset:
        """
        Load data from a GEOS product file into an xarray.Dataset.

        Args:
            rec: A file record pointing to the file to open.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec, product=self)

        if rec.local_path is None:
            rec = rec.get()

        return xr.load_dataset(rec.local_path)


inst3_3d_asm_nv_fc = GEOSForecastProduct("inst3_3d_asm_Nv")
tavg1_2d_lnd_nx_fc = GEOSForecastProduct("tavg1_2d_lnd_Nx")
tavg1_2d_flx_nx_fc = GEOSForecastProduct("tavg1_2d_flx_Nx")
tavg1_2d_rad_nx_fc = GEOSForecastProduct("tavg1_2d_rad_Nx")
