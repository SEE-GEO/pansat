"""
pansat.products.dem.globe
=========================

This module provides a product class for the NOAA GLOB DEM dataset.
"""
import re
from datetime import datetime, timedelta
import gzip
from typing import Tuple, Optional
from pathlib import Path

import numpy as np
import xarray as xr

import pansat
from pansat.products import Product, FilenameRegexpMixin
from pansat.time import TimeRange
from pansat import geometry
from pansat.file_record import FileRecord


LAT_EXTENT = {
    0: (50, 90),
    1: (0, 50),
    2: (-50, 0),
    3: (-90, -50),
}

def get_lonlat_extent(filename: str) -> Tuple[float, float, float, float]:
    """
    Get longitude latitude extent of NOAA GLOBE file.

    Args:
        filename: The filename of the NOAA GLOBE file.

    Return:
        A tuple ``(lon_min, lat_min, lon_max, lat_max)`` containing the
        longitude and latitude coordinates of the lower-left and upper-right
        corner of the spatial coverage of the given file.
    """
    if filename.startswith("all"):
        return geometry.LonLatRect(-90, -180, 90, 180)

    tile_ind = (ord(filename[0][0]) - ord('a'))
    lat_min, lat_max = LAT_EXTENT.get(tile_ind // 4)
    col_ind = tile_ind % 4
    lon_min = -180 + 90 * col_ind
    lon_max = lon_min + 90
    return (lon_min, lat_min, lon_max, lat_max)



class GLOBE(FilenameRegexpMixin, Product):
    """
    Product class for NOAA GLOBE product.
    """
    def __init__(self):
        """
        Instantiate GLOBE product.
        """
        self.filename_regexp = re.compile(
            rf"((\w)|(all))10g?\.t?gz"
        )
        Product.__init__(self)


    @property
    def name(self) -> str:
        """
        The product name that uniquely identifies the product within pansat.
        """
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return ".".join([prefix, "globe"])

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Implements interface to extract temporal coverage of file.
        """
        return TimeRange(datetime(1970, 1, 1), datetime.now())


    def get_spatial_coverage(self, rec: FileRecord) -> geometry.Geometry:
        """
        Implements interface to extract spatial coverage of file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        extent = get_lonlat_extent(rec.filename)
        return geometry.LonLatRect(*extent)

    @property
    def default_destination(self):
        """
        The default destination for the NOAA GLOBE product is noaa/globe
        """
        return Path("noaa") / "globe"

    def __str__(self):
        return self.name

    def open(self, rec: FileRecord, slcs: Optional[dict[str, slice]] = None):
        """
        Open file as xarray dataset.

        Args:
            rec: A FileRecord whose local_path attribute points to a local
                NOAA GLOBE file to open.
            slcs: An optional dictionary of slices to use to subset the
                data to load.

        Return:
            An xarray.Dataset containing the loaded data.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        extent = get_lonlat_extent(rec.filename)

        if rec.filename[-3:] == "tgz":
            datasets = []
            with TemporaryDirectory() as tmp:
                tarfile.Tarfile(rec.local_path).extractall(path=tmp.name)
                files = sorted(list(Path(tmp.name).glob("**/*")))
                for path in files:
                    data = np.memmap(path, dtype="int16").reshape(-1, 10800)

                    lon_min, lat_min, lon_max, lat_max = get_lonlat_extent(path.name)
                    n_rows, n_cols = data.shape
                    lons = np.linspace(lon_min, lon_max, n_cols + 1)
                    lons = 0.5 * (lons[1:] + lons[:-1])
                    lats = np.linspace(lat_min, lat_max, n_rows + 1)
                    lats = 0.5 * (lats[1:] + lats[:-1])[::-1]
                    dataset = xr.Dataset({
                        "longitude": (("longitude",), lons),
                        "latitude": (("latitude",), lats),
                        "elevation": (("latitude", "longitude"), data)
                    })
                    datasets.append(dataset)
            dataset = xr.merge(datasets)
        else:
            data = np.frombuffer(
                gzip.open(rec.local_path).read(),
                dtype="int16"
            ).reshape(-1, 10800)
            lon_min, lat_min, lon_max, lat_max = get_lonlat_extent(rec.filename)
            n_rows, n_cols = data.shape
            lons = np.linspace(lon_min, lon_max, n_cols + 1)
            lons = 0.5 * (lons[1:] + lons[:-1])
            lats = np.linspace(lat_min, lat_max, n_rows + 1)
            lats = 0.5 * (lats[1:] + lats[:-1])[::-1]
            dataset = xr.Dataset({
                "longitude": (("longitude",), lons),
                "latitude": (("latitude",), lats),
                "elevation": (("latitude", "longitude"), data)
            })
        return dataset


globe = GLOBE()
