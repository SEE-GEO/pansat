"""
pansat.products.stations.wegener_net
====================================

This module provides a product class for data from the WegenerNet stations.
"""

from datetime import datetime
from pathlib import Path
import re
from typing import List, Optional

import shapely
import numpy as np
import pandas as pd
import xarray as xr

import pansat
from pansat.file_record import FileRecord
from pansat.time import TimeRange
from pansat.products import Product, FilenameRegexpMixin
from pansat import geometry


_STATION_DATA = None


def get_station_data() -> xr.Dataset:
    """
    Get xarray.Dataset containing the station data.
    """
    global _STATION_DATA
    file_path = Path(__file__).parent / "files" / "wegener_stations.txt"
    if _STATION_DATA is None:
        data_frame = pd.read_csv(file_path, parse_dates=["Valid from"])
        dataset = xr.Dataset.from_dataframe(data_frame)
        dataset = dataset.rename({
            "index": "station",
            "Number": "number",
            "Valid from": "valid_from",
            "Latitude [°]": "latitude",
            "Longitude [°]": "longitude",
            "Altitude [m]": "altitude",
            "Slope [°]": "slope",
            "Orientation [°]": "orientation",
            "Locationclass": "location_class",
            "Surrounding area": "surrounding_area",
        })

        lons = dataset.longitude.data
        lats = dataset.latitude.data
        valid = (
            (-180 <= lons) * (lons <= 180) *
            (-90 <= lats) * (lats <= 90)
        )
        dataset = dataset[{"station": valid}]
        _STATION_DATA = dataset

    return _STATION_DATA


class WegenerNetStationFile(FilenameRegexpMixin, Product):
    """
    Class representing data from WegenerNet stations.
    """

    def __init__(self, stations: Optional[List[int]] = None):
        self._name = "station_data"
        self.stations = stations
        super().__init__()

        self.filename_regexp = re.compile(
            rf"WN_L2_V._HD_St\d+_([\w\d\-]*)_([\w\d\-]*)_UTC.csv"
        )

    @property
    def name(self) -> str:
        """
        The product name that uniquely identifies the product within pansat.
        """
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return ".".join([prefix, self._name])

    def filename_to_date(self, filename):
        """
        Extract timestamp from filename.

        Args:
            filename(``str``): Filename of a NOAA GRAASP file.

        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """

        return date

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Implements interface to extract temporal coverage of file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)
        match = self.filename_regexp.match(rec.filename)
        if match is None:
            raise RuntimeError(
                f"Provided file record with filename {rec.filename} doest not "
                " match the products filename regexp "
                f"{self.filename_regexp.pattern}. "
            )

        start_date = datetime.strptime(match.group(1), "%Y-%m-%dd%Hh%Mm")
        end_date = datetime.strptime(match.group(2), "%Y-%m-%dd%Hh%Mm")

        return TimeRange(start_date, end_date)

    def get_spatial_coverage(self, rec: FileRecord) -> geometry.Geometry:
        """
        Implements interface to extract spatial coverage of file.
        """
        station_data = get_station_data()
        if self.stations is not None:
            station_data = station_data.stations[{"station": self.stations}]

        point_coords = np.stack(
            [station_data.longitude.data, station_data.latitude.data], -1
        )
        geom = shapely.MultiPoint(point_coords)
        return geometry.ShapelyGeometry(geom)

    @property
    def default_destination(self):
        """
        Not used since data is not publicly available.
        """
        return Path("wegener_net")

    def __str__(self):
        return self.name

    def open(self, rec: FileRecord, slcs: Optional[dict[str, slice]] = None) -> xr.Dataset:
        """
        Open file as xarray dataset.

        Args:
            rec: A FileRecord whose local_path attribute points to a local NOAA GRAASP file to open.
            slcs: An optional dictionary of slices to use to subset the
                data to load.

        Return:
            An xarray.Dataset containing the loaded data.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        file_path = rec.local_path

        data_frame = pd.read_csv(
            file_path, parse_dates=["Time [YYYY-MM-DD HH:MM:SS UTC]"]
        )
        dataset = xr.Dataset.from_dataframe(data_frame)
        dataset = dataset.rename(
            {
                "Station": "station",
                "Time [YYYY-MM-DD HH:MM:SS UTC]": "time",
                "Precipitation [mm]": "surface_precip",
            }
        )[["station", "time", "surface_precip"]]
        dataset = dataset.swap_dims({"index": "time"}).drop_vars("index")
        dataset["station"] = dataset["station"][0]
        dataset = dataset.set_coords("time")

        station_data = get_station_data()[{"station": dataset.station}]
        dataset["latitude"] = station_data.latitude.data
        dataset["longitude"] = station_data.longitude.data
        return dataset


station_data = WegenerNetStationFile()
