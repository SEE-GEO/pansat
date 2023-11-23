"""
pansat.products.satellite.ncei.patmos_x
=======================================

Interface to access PATMOS-x CDR data.

"""
from datetime import datetime, timedelta
import re

import xarray as xr

import pansat
from pansat import TimeRange, FileRecord
from pansat.products import (
    Product,
    FilenameRegexpMixin
)
from pansat.geometry import LonLatRect

class PATMOSXProduct(FilenameRegexpMixin, Product):
    """
    Class representing the PATMOS-x AVHRR HIRS reflectance and cloud
    properties CDR provided by NOAA NCEI
    """
    def __init__(self, sensor=None, orbit=None):
        self.sensor = None
        if sensor is None:
            sensor = "[\w-]*"
        self.orbit = orbit
        if orbit is None:
            orbit = "\w*"

        self.filename_regexp = re.compile(
            f"patmosx_v\d\dr\d\d(-\w*)?_{sensor}_{orbit}_"
            f"d(?P<date>\d{{8}})_c\d{{8}}.nc"
        )

    @property
    def name(self):
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        name = "patmosx"
        if self.sensor is not None:
            name += f"_{self.sensor}"
        if self.orbit is not None:
            name += f"_{self.orbit}"
        return prefix + "." + name

    @property
    def default_destination(self):
        """Default destination for downloads."""
        return f"patmosx"

    def get_temporal_coverage(self, rec):
        """
        Args:
            filename: The name of GridSat file.
        Return:
            ``datetime.datetime`` object of the corresponding
            time.
        """
        if not isinstance(rec, FileRecord):
            rec = FileRecord(local_path=rec)
        match = self.filename_regexp.match(rec.filename)
        if match is None:
            raise ValueError(
                "Provided file record doesn't match filename patter of "
                f"the {self.name} product."
            )
        date = match.group("date")
        start = datetime.strptime(date, "%Y%m%d")
        end = start + timedelta(hours=23, minutes=59, seconds=59)
        return TimeRange(start, end)

    def get_spatial_coverage(self, rec):
        """
        Args:
            rec: A FileRecord pointing to a local PATMOS-X CDR file.
        Return:
            A geometry object representing the spatial coverage of the
            data.
        """
        # Coverage of PATMOS-x files is global.
        return LonLatRect(-180, -90, 180, 90)

    def open(self, rec) -> xr.Dataset:
        """
        Args:
            rec: A FileRecord pointing to a local PATMOS-x CDR file.
        Return:
            The data in the data file loaded into an xarray.Dataset.
        """
        return xr.load_dataset(rec.local_path)


patmosx = PATMOSXProduct()
patmosx_asc = PATMOSXProduct(orbit="asc")
patmosx_des = PATMOSXProduct(orbit="des")
