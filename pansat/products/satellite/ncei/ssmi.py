"""
pansat.products.satellite.ncei.ssmi
===================================

Products representing the NOAA NCEI microwave brightness temperature
CDRs derived from SSMI and SSMIS observations.
"""
from datetime import datetime
from pathlib import Path
import re

import xarray as xr

import pansat
from pansat.products import (
    Product,
    FilenameRegexpMixin
)
from pansat import geometry
from pansat.time import TimeRange


class SSMIProduct(FilenameRegexpMixin, Product):
    def __init__(self, variant):
        self.variant = variant
        self.filename_regexp = re.compile(
            rf"{variant.upper()}_SSMI_FCDR_V\w*_\w*_D(\d{{8}})_S(\d{{4}})"
            r"_E(\d{4})_\w*.nc"
        )

    @property
    def name(self):
        """
        The product name that uniquely identifies the product within pansat.
        """
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return prefix + f".ssmi_{self.variant}"

    @property
    def default_destination(self):
        """Default destination for downloads."""
        return f"ssmi_{self.variant}"

    def get_temporal_coverage(self, rec):
        """
        Args:
            rec: A FileRecord pointing to a remote or local SSMI CDR file.
        Return:
            A TimeRange object representing the temporal coverage of the given
            file.
        """
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
            lons = data["lon_lores"].data
            lats = data["lat_lores"].data
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
