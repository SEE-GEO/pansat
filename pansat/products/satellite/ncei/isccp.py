"""
pansat.products.ncei.isccp
============================

Provides products representing the international satellite cloud climate
project (ISCCP).
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


class ISCCPBasicProduct(FilenameRegexpMixin, Product):
    """
    Class for ISCCP products.
    """

    def __init__(self, variant: str, temporal_resolution: timedelta):
        """
        Args:
            variant: The variant of the GridSat product: 'conus' or 'goes'.
            temporal_resolution: timdelta object representing the temporal
                resolution of the product.
        """
        self.variant = variant.lower()
        self.temporal_resolution = temporal_resolution
        self.filename_regexp = re.compile(
            rf"ISCCP-Basic\.{variant.upper()}\.v01r00\.GLOBAL\."
            r"(\d{4}\.\d{2})\.(\d{2})\.(\d{4})\.[\w\.]*\.nc"
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
        return prefix + f".isccp_{self.variant}"

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Args:
            filename: The name of GridSat file.
        Return:
            ``datetime.datetime`` object of the corresponding
            time.
        """
        if not isinstance(rec, FileRecord):
            rec = FileRecord(rec)
        match = self.filename_regexp.match(rec.filename)
        if match is None:
            raise ValueError(
                f"The provided file record '{rec}' does not point to a "
                f"{self.name} file."
            )
        if match.group(3) == "9999":
            if match.group(2) == "99":
                time_stamp = datetime.strptime(match.group(1), "%Y.%m")
            else:
                time_stamp = datetime.strptime(f"{match.group(1)}.{match.group(2)}", "%Y.%m.%d")
        else:
            time_stamp = datetime.strptime(
                f"{match.group(1)}.{match.group(2)}.{match.group(3)}",
                "%Y.%m.%d.%H%M"
            )
        start_time = time_stamp - 0.5 * self.temporal_resolution
        end_time = time_stamp + 0.5 * self.temporal_resolution
        return TimeRange(start_time, end_time)

    def get_spatial_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Args:
            filename: The name of ISCCP file.
        Return:
            A geometry object representing the spatial coverage of the file.
        """
        return LonLatRect(-180, -60, -50, 60)

    @property
    def default_destination(self):
        """Default destination for downloads."""
        return "isccp"

    def open(self, rec: FileRecord):
        """Open given file as ``xarray.Dataset``."""
        return xr.open_dataset(rec.local_path)


isccp_hgg = ISCCPBasicProduct("hgg", timedelta(hours=3))
isccp_hgm = ISCCPBasicProduct("hgm", timedelta(days=31))


class ISCCPProduct(ISCCPBasicProduct):
    """
    Class for ISCCP products.
    """

    def __init__(self, variant: str, temporal_resolution: timedelta):
        """
        Args:
            variant: The variant of the GridSat product: 'conus' or 'goes'.
            temporal_resolution: timdelta object representing the temporal
                resolution of the product.
        """
        super().__init__(variant=variant, temporal_resolution=temporal_resolution)
        Product.__init__(self)
        self.filename_regexp = re.compile(
            rf"ISCCP\.{self.variant.upper()}\.v01r00\.GLOBAL\."
            r"(\d{4}\.\d{2})\.(\d{2})\.(\d{4})\.[\w\.]*\.nc"
        )

isccp_hxg = ISCCPProduct("hxg", timedelta(hours=3))
