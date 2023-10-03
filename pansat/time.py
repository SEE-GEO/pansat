"""
pansat.time
===========

This module provides functionality for dealing with times.
"""
from datetime import datetime, timedelta
from dataclasses import dataclass
import json
from typing import Union

import pandas as pd
import numpy as np


Time = Union[str, datetime, np.ndarray]


def to_datetime(time):
    """
    Try to convert a given time to a datetime object.
    """
    if isinstance(time, datetime):
        return time
    try:
        return pd.to_datetime(time).to_pydatetime()
    except ValueError:
        raise ValueError(
            f"Could not convert '{time}' to datetime object.",
        )


def to_datetime64(time):
    """
    Try to convert a given time to a numpy datetime64 object.
    """
    if isinstance(time, np.ndarray) and time.dtype == np.datetime64:
        return time
    try:
        return pd.to_datetime(time).to_datetime64()
    except ValueError:
        raise ValueError("Could not convert '%s' to datetime object.")


@dataclass
class TimeRange:
    """
    A time range defining the temporal extent of a dataset.

    The time range is represented by the 'start' and 'end' attributes
    of the class representing the start and end of the time range covered
    by a given data file.

    If the temporal extent of a data file cannot be deduced from the
    filename alone or it is not known, the 'end' attribute can be 'None'.
    """

    start: np.datetime64
    end: np.datetime64 = None

    def __init__(self, start, end):
        """
        Create a time range from a given start and end time.

        Mathematically, the time interval is closed. This means that both
        start and end point are considered to lie within the interval.

        Args:
            start: The start time of the time range as a string, Python
                datetime object or a numpy.datetime64 object.
            end: The end time of the time range as a string, Python
                datetime object or a numpy.datetime64 object.
        """
        if isinstance(start, str):
            start = np.datetime64(start)
        if isinstance(end, str):
            end = np.datetime64(end)
        self.start = to_datetime(start)
        self.end = to_datetime(end)

    def __eq__(self, other):
        if not isinstance(other, TimeRange):
            return NotImplemented
        return self.start == other.start and self.end == other.end

    def covers(self, time: Union[Time, "TimeRange"]) -> bool:
        """
        Determins wether time range covers a certain time.

        Args:
            time: A single time or a time range for which to check coverage
                with this time range.

        Return:
            'True' if the 'time' lies within the time range represented by
            this object. 'False' otherwise.
        """
        if isinstance(time, TimeRange):
            return not ((self.start > time.end) or (self.end < time.start))
        time = to_datetime(time)
        return (time >= self.start) and (time <= self.end)

    @classmethod
    def from_dict(cls, dct):
        """Create TimeRange object from dictionary representaiton."""
        return TimeRange(dct["start"], dct["end"])

    def to_dict(self):
        """
        Return a dictionary representation containing only primitive types.
        """
        return {"start": self.start.isoformat(), "end": self.end.isoformat()}

    def to_json(self):
        """Return json representation of time range object."""
        return json.dumps({"TimeRange": self.to_dict()})

    def __repr__(self):
        start = self.start.isoformat()
        end = self.end.isoformat()
        return f"TimeRange(start='{start}', end='{end}')"

    def __lt__(self, other):
        """
        TimeRange objects are compared by their start time.
        """
        return self.start <= other.start

    def __iter__(self):
        """
        Iterate over start and end of the time range.
        """
        yield self.start
        yield self.end

    def expand(self, delta):
        """
        Expand time range.

        Args:
            delta: A time delta specifying the time by which to expand the
                time range. This should be a 'datetime.timedelta', a numpy
                timedelta64 object or a tuple. If it is a tuple, it should
                contain two deltas. The first of these deltas will be
                subtracted from the start of the range and the second will
                added to the end of the range.

        Return:
            A new TimeRange object expanded by the given timedelta.
        """
        if isinstance(delta, tuple):
            left, right = delta
        else:
            left = delta
            right = delta
        if isinstance(left, timedelta):
            start = to_datetime(self.start) - left
            end = to_datetime(self.end) + right
        elif isinstance(left, np.timedelta64):
            start = to_datetime64(self.start) - left
            end = to_datetime64(self.end) + right
        else:
            raise ValueError(
                "Time deltas must be 'datetime.timedelta' or numpy "
                "datetime64 objects."
            )
        return TimeRange(start, end)
