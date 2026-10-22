"""
pansat.products.satellite.ncei.cmorph
======================================

Products for CMORPH high-resolution global precipitation estimates.
"""
from datetime import datetime, timedelta
from pathlib import Path
import re

import xarray as xr
import pansat
from pansat.file_record import FileRecord
from pansat.geometry import LonLatRect
from pansat.products import Product, FilenameRegexpMixin
from pansat.time import TimeRange


class CmorphProduct(FilenameRegexpMixin, Product):
    """
    CMORPH V1.0 high-resolution global precipitation estimates.

    Files are named CMORPH_V1.0_ADJ_{spatial}-{temporal}_YYYYMMDDHHMM.nc
    and are organized on NCEI by year/month/day.
    """

    def __init__(self, spatial_resolution: str, temporal_resolution: str):
        """
        Args:
            spatial_resolution: Spatial resolution string, e.g. '8km' or '0.25deg'.
            temporal_resolution: Temporal resolution string, e.g. '30min' or 'daily'.
        """
        self.spatial_resolution = spatial_resolution
        self.temporal_resolution_str = temporal_resolution
        self.filename_regexp = re.compile(
            rf"CMORPH_V1\.0_ADJ_{re.escape(spatial_resolution)}"
            rf"-{re.escape(temporal_resolution)}_\d{{10,12}}\.nc"
        )
        Product.__init__(self)

    @property
    def name(self):
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        res_str = (
            f"{self.spatial_resolution}_{self.temporal_resolution_str}"
            .replace(".", "p")
        )
        return prefix + f".cmorph_{res_str}"

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Args:
            rec: A file record identifying a CMORPH file.
        Return:
            A TimeRange representing the temporal coverage of the file.
        """
        ts = rec.filename.split("_")[-1].replace(".nc", "")
        if len(ts) == 12:
            # YYYYMMDDHHMM
            dt = datetime(int(ts[:4]), int(ts[4:6]), int(ts[6:8]),
                          int(ts[8:10]), int(ts[10:12]))
            half_width = timedelta(minutes=30)
        elif len(ts) == 10:
            # YYYYMMDDHH
            dt = datetime(int(ts[:4]), int(ts[4:6]), int(ts[6:8]),
                          int(ts[8:10]))
            half_width = timedelta(minutes=30)
        else:
            # YYYYMMDD (daily)
            dt = datetime(int(ts[:4]), int(ts[4:6]), int(ts[6:8]))
            half_width = timedelta(hours=12)
        return TimeRange(dt - half_width, dt + half_width)

    def get_spatial_coverage(self, rec: FileRecord) -> LonLatRect:
        """Global coverage."""
        return LonLatRect(-180, -90, 180, 90)

    @property
    def default_destination(self):
        return f"cmorph_{self.spatial_resolution}_{self.temporal_resolution_str}"

    def open(self, rec: FileRecord):
        """Open file as ``xarray.Dataset``."""
        ds = xr.open_dataset(rec.local_path)
        ds = ds.rename({k: v for k, v in {"lon": "longitude", "lat": "latitude"}.items() if k in ds})
        if ds["longitude"].values.max() > 180:
            ds = ds.assign_coords(longitude=(ds["longitude"] + 180) % 360 - 180).sortby("longitude")
        return ds


cmorph_8km_30min = CmorphProduct("8km", "30min")
