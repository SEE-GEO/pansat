"""
pansat.products.ground_based.opera
==================================

This module defines the ``OperaProduct`` class, which is used to represent data
products from the EUMETNET Opera ground-radar network.
"""
from datetime import datetime, timedelta
from pathlib import Path
import re
from tarfile import TarFile
from tempfile import TemporaryDirectory
from typing import Union

import numpy as np
import pyproj
import xarray as xr

import pansat
from pansat import FileRecord, TimeRange
from pansat.time import to_datetime
import pansat.download.providers as providers
from pansat.geometry import Geometry, Polygon
from pansat.products import FilenameRegexpMixin, Product
from pansat.products.product_description import ProductDescription


OPERA_NAMES = {
    "precip_rate": "RAINFALL_RATE",
    "reflectivity": "REFLECTIVITY",
}


OPERA_DOMAIN = Polygon(
    [
        [-10.434576838640398, 31.746215319325056],  # lower left
        [29.421038635578032, 31.98765027794496],  # lower right
        [57.81196475014995, 67.62103710275053],  # upper right
        [67.02283275830867, -39.5357864125034],  # upper left
        [-10.434576838640398, 31.746215319325056],  # lower left
    ]
)


class OperaProduct(FilenameRegexpMixin, Product):
    """
    Class representing Opera products.

    OPERA is the Radar Programme of EUMETNET. More information can be found on
    the `EUMETNET <https://www.eumetnet.eu/activities/observations-programme/current-activities/opera/>`_ homepage.
    """

    def __init__(self, product_name, description):
        self.product_name = product_name
        self.description = description
        self.filename_regexp = re.compile(rf"(\d{{8}})_{OPERA_NAMES[product_name]}.tar")

    @property
    def name(self):
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return ".".join([prefix, self.product_name])

    @property
    def default_destination(self) -> Path:
        return Path("opera")

    def filename_to_date(self, filename):
        """
        Extract timestamp from filename.

        Args:
            filename(``str``): Filename of the OPERA product.

        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """
        path = Path(filename)
        match = self.filename_regexp.match(path.name)
        date = datetime.strptime(match.group(1), "%Y%m%d")
        return date

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Return temporal coverage of OPERA file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(Path(rec))

        start_time = self.filename_to_date(rec.filename)
        end_time = start_time + timedelta(days=1)
        return TimeRange(start_time, end_time)

    def get_spatial_coverage(self, rec: FileRecord) -> Geometry:
        """
        Returns a polygon representation of the OPERA domain.
        """
        return OPERA_DOMAIN

    def get_filename(self, time: Union[datetime, np.datetime64]) -> str:
        """
        Determine filename of opera product for a given day.

        Args:
            time: A time stamp specifying the current day.

        Return:
            A string specifying the filename of a data file of this opera
            product for the given day.
        """
        date = to_datetime(time)
        date_str = date.strftime("%Y%m%d")
        filename = f"{date_str}_{OPERA_NAMES[self.product_name]}.tar"
        return filename

    def open(self, rec: FileRecord) -> xr.Dataset:
        """
        Open OPERA files as xarray.Dataset

        Args:
            rec: A FileRecord object whose local_path attribute points
                to a local OPERA product file to load.

        Return:
            An xarray.Dataset containing the loaded data.
        """
        from pansat.formats.hdf5 import HDF5File

        with TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            TarFile(rec.local_path).extractall(tmp)
            files = sorted(list(tmp.glob("*h5")))
            datasets = []
            for opera_file in files:
                time = datetime.strptime(
                    opera_file.name.split("_")[1][:-3], "%Y%m%d%H%M"
                )
                with HDF5File(opera_file, "r") as file_handle:
                    data = self.description.to_xarray_dataset(file_handle, globals())
                data["time"] = time
                datasets.append(data)

        return xr.concat(datasets, dim="time")


def edges_to_centers(grid):
    new_grid = 0.25 * (grid[1:, 1:] + grid[1:, :-1] + grid[:-1, 1:] + grid[:-1, :-1])
    return new_grid


_LATITUDE_GRID = None
_LONGITUDE_GRID = None


def _define_coordinate_grids():
    projection_string = (
        "+proj=laea +lat_0=55.0 +lon_0=10.0 +x_0=1950000.0 "
        "+y_0=-2100000.0 +units=m +ellps=WGS84"
    )
    proj = pyproj.Proj(projection_string)
    x = np.arange(0, 1900 * 2000.0 + 1, 2000.0)
    y = np.arange(0, -2200 * 2000.0 - 1, -2000.0)
    x, y = np.meshgrid(x, y)
    lons, lats = proj(x.ravel(), y.ravel(), inverse=True)
    lats = lats.reshape(2201, 1901)
    lons = lons.reshape(2201, 1901)

    global _LATITUDE_GRID
    global _LONGITUDE_GRID
    _LATITUDE_GRID = edges_to_centers(lats)
    _LONGITUDE_GRID = edges_to_centers(lons)
    _LATITUDE_GRID.flags.writeable = False
    _LONGITUDE_GRID.flags.writeable = False


def _get_opera_projection():
    projection_string = (
        "+proj=laea +lat_0=55.0 +lon_0=10.0 +x_0=1950000.0 "
        "+y_0=-2100000.0 +units=m +ellps=WGS84"
    )
    proj = pyproj.Proj(projection_string)
    return proj


def get_latitude_grid(*args):
    """
    Returns the latitude grid on which the Opera data is provided.
    """
    global _LATITUDE_GRID
    if _LATITUDE_GRID is None:
        _define_coordinate_grids()
    return _LATITUDE_GRID


def get_longitude_grid(*args):
    """
    Returns the longitude grid on which the Opera data is provided.
    """
    global _LONGITUDE_GRID
    if _LONGITUDE_GRID is None:
        _define_coordinate_grids()
    return _LONGITUDE_GRID


def _parse_products():
    """
    Parses available Opera products.
    """
    module_path = Path(__file__).parent
    for filename in module_path.iterdir():
        if filename.match("*.ini"):
            description = ProductDescription(filename)
            python_name = filename.name.split(".")[0]
            product_name = description.name
            globals()[python_name] = OperaProduct(product_name, description)


_parse_products()
