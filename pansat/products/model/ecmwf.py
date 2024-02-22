"""
Datasets provided by ECMWF
==========================

This model implements products representing data from the TIGGE and S2S
ECMWF datasets.
"""
from calendar import monthrange
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import xarray as xr

from pansat.file_record import FileRecord
from pansat.time import TimeRange
from pansat.products import Product
from pansat.geometry import LonLatRect


PARAMETERS = {
    "total_precip": "228228"
}


def get_weekdays(
        start: datetime,
        end: datetime,
        days: List[int]
) -> List[datetime]:
    """
    List all dates corresponding a specific selections of weekdays in a given
    time range.

    Args:
        start: A datetime object specifying the start of the time interval.
        end: A datetime object specifying the end of the time interval.
        days: List of integer specifying the week days to which the
            returned dates should correspond.

    Return:
        A list of datetime object listing all dates within the given time
        range that correspond to the selected weekdays.
    """
    time = start
    dates = []
    while time < end:
        if time.weekday() in days:
            dates.append(time)
        time = time + timedelta(days=1)
    return dates

    
class ECMWFDataset(Product):
    """
    Dataset class for data available from ECMWF.
    """
    def __init__(
            self,
            clss: str,
            dataset: str,
            origin: str,
            variable: str = "total_precip",
            number: Optional[int] = None
    ):
        self.clss = clss
        self.dataset = dataset
        self.origin = origin
        self.variable = variable
        if number is None:
            self.filename_pattern = (
                f"{self.dataset}_{self.origin}_{self.variable}_%Y%m.grib"
            )
        else:
            self.filename_pattern = (
                f"{self.dataset}_{self.origin}_{self.variable}_{number}_%Y%m.grib"
            )
        self.number = number
        super().__init__()

    @property
    def name(self):
        name = f"model.ecmwf.{self.dataset}_{self.origin}_{self.variable}"
        if self.number is not None:
            name += f"_{self.number:02}"
        return name

    @property
    def default_destination(self) -> Path:
        return Path(f"ecmwf/{self.dataset}")

    def temporal_extent(self, time=None):
        if time is None:
            return timedelta(days=31)
        _, n_days = monthrange(time.year, time.month)
        return timedelta(days=n_days)

    def get_request(
            self,
            rec: TimeRange
    ):
        time_range = rec.temporal_coverage

        dates = get_weekdays(time_range.start, time_range.end, [0, 3])
        dates = "/".join(date.strftime("%Y-%m-%d") for date in dates)
        start_date = time_range.start.strftime("%Y-%m-%d")
        month_days = monthrange(time_range.start.year, time_range.start.month)[1]
        end_date = time_range.start + timedelta(days=month_days - 1)
        end_date = end_date.strftime("%Y-%m-%d")
        dates = start_date + "/to/" + end_date

        steps = range(0, 720, 6)
        if self.origin == "egrr":
            steps = steps[1:]

        request = {
            "class": self.clss,
            "dataset": self.dataset,
            "date": dates,
            "expver": "prod",
            "levtype": "sfc",
            "model": "glob",
            "origin": self.origin,
            "param": PARAMETERS[self.variable],
            "step": "/".join(map(str, steps)),
            "stream": "enfo",
            "time": "00:00:00",
            "type": "cf",
        }
        if self.number is not None:
            request["number"] = "/".join(map(str, range(1, self.number + 1)))
            request["type"] = "pf"
            request["target"] = "output"

        return request

    def get_filename(self, date):
        return date.strftime(self.filename_pattern)


    def matches(self, rec: FileRecord) -> bool:
        return True


    def get_temporal_coverage(self, rec: FileRecord):
        if not isinstance(rec, FileRecord):
            rec = FileRecord(rec)

        start = datetime.strptime(
            rec.filename.split("_")[-1],
            "%Y%m.grib"
        )
        end = start + self.temporal_extent(start)
        return TimeRange(
            start,
            end
        )


    def get_spatial_coverage(self, rec: FileRecord):
        return LonLatRect(-180, -90, 180, 90)


    def open(self, rec: FileRecord) -> xr.Dataset:
        """
        Open data in file record.

        Args:
            rec: A file record with a 'local_path' attribute pointing
                to the file to open.

        Return:
            The forecast data from the local product file identified
            by the given file record.

        """
        if not isinstance(rec, FileRecord):
            rec = FileRecord

        data = xr.load_dataset(rec.local_path)

        lons = data.longitude.data
        lons = (lons + 180.0) % 360.0 - 180.0
        data.coords["longitude"] = lons
        data = data.sortby("longitude")
        return data


s2s_ecmwf_total_precip = ECMWFDataset(
    "s2",
    "s2s",
    "ecmf",
    "total_precip"
)

s2s_ecmwf_total_precip_10 = ECMWFDataset(
    "s2",
    "s2s",
    "ecmf",
    "total_precip",
    number=10
)

s2s_ecmwf_total_precip_50 = ECMWFDataset(
    "s2",
    "s2s",
    "ecmf",
    "total_precip",
    number=50
)

s2s_ecmwf_total_precip_10 = ECMWFDataset(
    "s2",
    "s2s",
    "ecmf",
    "total_precip",
    number=10
)

s2s_ecmwf_total_precip_50 = ECMWFDataset(
    "s2",
    "s2s",
    "ecmf",
    "total_precip",
    number=50
)

s2s_ukmo_total_precip = ECMWFDataset(
    "s2",
    "s2s",
    "egrr",
    "total_precip"
)

s2s_ukmo_total_precip_3 = ECMWFDataset(
    "s2",
    "s2s",
    "egrr",
    "total_precip",
    number=3
)
