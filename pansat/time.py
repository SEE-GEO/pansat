"""
pansat.time
===========

This module provides functionality for dealing with times.
"""
from datetime import datetime
from dataclasses import dataclass
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
        raise ValueError("Could not convert '%s' to datetime object.")


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
        self.start = to_datetime(start)
        self.end = to_datetime(end)

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
