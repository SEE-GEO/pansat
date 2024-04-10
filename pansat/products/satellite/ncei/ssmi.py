"""
pansat.products.satellite.ncei.ssmi
===================================

Products representing the NOAA NCEI microwave brightness temperature
CDRs derived from SSMI and SSMIS observations.
"""
from datetime import datetime, timedelta
from pathlib import Path
import re

import xarray as xr

import pansat
from pansat.products import (
    Product,
    FilenameRegexpMixin
)
from pansat import FileRecord
from pansat import geometry
from pansat.time import TimeRange
from pansat.geometry import LonLatRect


class SSMIProduct(FilenameRegexpMixin, Product):
    def __init__(self, variant, sensor="SSMI"):
        """
        Args:
            variant: The variant of the SSMI record 'rss' or 'csu'.
            sensor: The sensor from which the record is derived.
        """
        self.variant = variant.lower()
        self.sensor = sensor.lower()
        if sensor.lower() == "all":
            sensor = r"\w*"
        else:
            sensor = sensor.upper()
        self.filename_regexp = re.compile(
            rf"{variant.upper()}_{sensor}_FCDR_V\w*_\w*_D(\d{{8}})_S(\d{{4}})"
            r"_E(\d{4})_\w*.nc"
        )
        super().__init__()

    @property
    def name(self):
        """
        The product name that uniquely identifies the product within pansat.
        """
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return prefix + f".{self.sensor}_{self.variant}"

    @property
    def default_destination(self):
        """Default destination for downloads."""
        return f"{self.sensor}_{self.variant}"

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Args:
            rec: A FileRecord pointing to a remote or local SSMI CDR file.
        Return:
            A TimeRange object representing the temporal coverage of the given
            file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        match = self.filename_regexp.match(rec.filename)
        if match is None:
            raise ValueError(
                "Provided file record doesn't match filename patter of "
                f"the {self.name} product."
            )
        date = match.group(1)
        start = match.group(2)
        end = match.group(3)
        start = datetime.strptime(date + start, "%Y%m%d%H%M")
        end = datetime.strptime(date + end, "%Y%m%d%H%M")
        return TimeRange(start, end)


    def get_spatial_coverage(self, rec):
        """
        Args:
            rec: A FileRecord pointing to a local SSMI CDR file.
        Return:
            A geometry object representing the spatial coverage of the
            data.
        """
        if rec.local_path is None:
            raise ValueError(
                "This products reuqires a local file is to determine "
                " the spatial coverage."
            )

        with xr.open_dataset(rec.local_path) as data:
            if "lat_lores" in data:
                lons = data["lon_lores"].data
                lats = data["lat_lores"].data
            else:
                lons = data["lon"].data
                lats = data["lat"].data
        poly = geometry.lonlats_to_polygon(lons, lats, n_points=40)
        return poly


    def open(self, rec) -> xr.Dataset:
        """
        Args:
            rec: A FileRecord pointing to a local SSMI CDR file.
        Return:
            The data in the data file loaded into an xarray.Dataset.
        """
        return xr.load_dataset(rec.local_path)


ssmi_csu = SSMIProduct("csu")
ssmi_rss = SSMIProduct("rss")


class SSMIGriddedProduct(SSMIProduct):
    def __init__(self, variant, sensor="ssmi"):
        """
        Args:
            variant: The variant of the SSMI record 'rss' or 'csu'.
            sensor: The sensor from which the record is derived.
        """
        self.variant = variant
        super().__init__(variant, sensor=sensor)
        if sensor.lower() == "all":
            sensor = r"\w*"
        else:
            sensor = sensor.upper()
        self.filename_regexp = re.compile(
            rf"{variant.upper()}_{sensor}_FCDR-GRID_V\w*_\w*_D(\d{{8}}).nc"
        )

    @property
    def name(self):
        """
        The product name that uniquely identifies the product within pansat.
        """
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return prefix + f".ssmi_{self.variant}_gridded"

    @property
    def default_destination(self):
        """Default destination for downloads."""
        return f"ssmi_{self.variant}"

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Args:
            rec: A FileRecord pointing to a remote or local SSMI CDR file.
        Return:
            A TimeRange object representing the temporal coverage of the given
            file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        match = self.filename_regexp.match(rec.filename)
        if match is None:
            raise ValueError(
                "Provided file record doesn't match filename patter of "
                f"the {self.name} product."
            )
        date = match.group(1)
        start = datetime.strptime(date, "%Y%m%d")
        end = start + timedelta(days=1)
        return TimeRange(start, end)


    def get_spatial_coverage(self, rec):
        """
        Args:
            rec: A FileRecord pointing to a local SSMI CDR file.
        Return:
            A geometry object representing the spatial coverage of the
            data.
        """
        return LonLatRect(
            -180, 180, -90, 90
        )


ssmi_csu_gridded = SSMIGriddedProduct("csu")
ssmi_csu_gridded_all = SSMIGriddedProduct("csu", sensor="all")
ssmis_csu_gridded = SSMIGriddedProduct("csu", sensor="ssmis")
amsr2_csu_gridded = SSMIGriddedProduct("csu", sensor="amsr2")
