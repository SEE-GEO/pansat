"""
Datasets provided by ECMWF
==========================

This model implements products representing data from the TIGGE and S2S
ECMWF datasets.
"""
from calendar import monthrange
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

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


    """
    def __init__(
            self,
            clss: str,
            dataset: str,
            origin: str,
            variable: str = "total_precip"
    ):
        self.clss = clss
        self.dataset = dataset
        self.origin = origin
        self.variable = variable
        self.filename_pattern = (
            f"{self.dataset}_{self.origin}_{self.variable}_%Y%m.grib"
        )
        super().__init__()

    @property
    def name(self):
        return f"model.ecmwf.{self.dataset}_{self.origin}_{self.variable}"


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
        print(time_range)
        dates = get_weekdays(time_range.start, time_range.end, [0, 3])
        dates = "/".join(date.strftime("%Y-%m-%d") for date in dates)

        request = {
            "class": self.clss,
            "dataset": self.dataset,
            "date": dates,
            "expver": "prod",
            "levtype": "sfc",
            "model": "glob",
            "origin": self.origin,
            "param": PARAMETERS[self.variable],
            "step": "/".join(map(str, range(0, 673, 6))),
            "stream": "enfo",
            "time": "00:00:00",
            "type": "cf",
        }
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


    def open(self, rec: FileRecord):

        if not isinstance(rec, FileRecord):
            rec = FileRecord

        return xr.load_dataset(rec.local_path)


s2s_ecmwf_total_precip = ECMWFDataset(
    "s2",
    "s2s",
    "ecmf",
    "total_precip"
)
