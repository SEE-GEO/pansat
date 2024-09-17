"""
pansat.time
===========

This module provides functionality for dealing with times.
"""
from datetime import datetime, timedelta
from dataclasses import dataclass
import json
from typing import Union, List

import pandas as pd
import numpy as np


Time = Union[str, datetime, np.datetime64]
TimeDelta = Union[timedelta, np.timedelta64]


def to_datetime(time):
    """
    Try to convert a given time to a datetime object.
    """
    if isinstance(time, datetime):
        return time
    try:
        if isinstance(time, np.ndarray) and np.issubdtype(time.dtype, np.datetime64):
            time = time.astype("datetime64[s]")
        return pd.to_datetime(time).to_pydatetime()
    except ValueError:
        raise ValueError(
            f"Could not convert '{time}' to datetime object.",
        )


def to_timedelta(d_t):
    """
    Convert time delta to Python datetime.timedelta object.
    """
    if isinstance(d_t, timedelta):
        return d_t
    try:
        return pd.Timedelta(d_t).to_pytimedelta()
    except ValueError:
        raise ValueError(
            f"Could not convert '{d_t}' to timedelta object.",
        )

def to_timedelta64(d_t):
    """
    Convert time delta to numpy timedelta64 object.
    """
    if hasattr(d_t, "dtype") and np.issubdtype(d_t.dtype, np.datetime64):
        return d_t
    try:
        return pd.Timedelta(d_t).to_timedelta64()
    except ValueError:
        raise ValueError(
            f"Could not convert '{d_t}' to timedelta object.",
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

    def __init__(self, start, end=None):
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
        if isinstance(start, TimeRange):
            return self.__init__(start.start, end)

        if end is None:
            end = start
        if isinstance(start, str):
            start = np.datetime64(start)
        if isinstance(end, str):
            end = np.datetime64(end)
        self.start = to_datetime(start)
        self.end = to_datetime(end)

    @staticmethod
    def to_time_range(time_range: Union[Time, "TimeRange"]) -> "TimeRange":
        if isinstance(time_range, TimeRange):
            return time_range
        try:
            time_range = TimeRange(time_range)
        except ValueError:
            raise ValueError(
               f"Could not convert object '{time_range}' to time range."
            )
        return time_range

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

    def __add__(self, delta):
        """
        Add time difference to start and end of time range.

        Args:
            delta: A time delta object specifying the time difference
                by which to shift the time range.

        Return:
            A new time range object whose start and end times were shifted
            by the given time difference.
        """
        if (
                not np.issubdtype(delta.dtype, np.timedelta64) and
                not isinstance(delta, timedelta)
        ):
            return NotImplemented
        return TimeRange(
            start=self.start + to_timedelta(delta),
            end=self.end + to_timedelta(delta),
        )

    def __sub__(self, delta):
        """
        Subtract time difference from start and end of time range.

        Args:
            delta: A time delta object specifying the time difference
                by which to shift the time range.

        Return:
            A new time range object whose start and end times were shifted
            by the given time difference.
        """
        if (
                not np.issubdtype(delta.dtype, np.timedelta64) and
                not isinstance(delta, timedelta)
        ):
            return NotImplemented
        delta = to_timedelta(delta)
        return TimeRange(
            start=self.start - delta,
            end=self.end - delta,
        )

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


    def find_closest(
            self,
            time_ranges: List["TimeRange"]
    ) -> List["TimeRange"]:
        """
        Return time ranges that cover this time range object, or, if no
        such range exists, return the time range that with the smallest
        time difference to this range's start or end time.

        Args:
            time_ranges: A list of condidate time ranges.

        Return:
            A list of time range object that either overlap with the
            given time range or the time range that minimizes the
            time difference between the starts and end of this and
            the returned time range.
        """
        closest = []
        min_delta = None
        range_ind = None

        for range_ind, other in enumerate(time_ranges):
            time_delta = max(
                self.start - other.end,
                other.start - self.end
            )
            if time_delta <= np.timedelta64(0, "s"):
                closest.append(other)
            else:
                if min_delta is None:
                    min_delta = time_delta
                    min_index = range_ind
                else:
                    if time_delta < min_delta:
                        min_delta = time_delta
                        min_index = range_ind

        if range_ind is not None and len(closest) == 0:
            closest.append(time_ranges[min_index])

        return closest


    def find_closest_ind(
            self,
            time_ranges: List["TimeRange"]
    ) -> List[int]:
        """
        Same as find closest but returns indices of the closest time
        ranges.

        Args:
            time_ranges: A list of candidate time ranges.

        Return:
            A list of indices identifying time range objects in
            'time_range' that either overlap with 'self'or, if
            'time_ranges' does not contain such a time range,
            the index of the element in 'time_ranges' that minimizes
            the time difference between the starts and end of this and
            the returned time range.
        """
        closest = []
        min_delta = None
        range_ind = None

        for range_ind, other in enumerate(time_ranges):
            time_delta = max(
                self.start - other.end,
                other.start - self.end
            )
            if time_delta <= np.timedelta64(0, "s"):
                closest.append(range_ind)
            else:
                if min_delta is None:
                    min_delta = time_delta
                    min_index = range_ind
                else:
                    if time_delta < min_delta:
                        min_delta = time_delta
                        min_index = range_ind

        if range_ind is not None and len(closest) == 0:
            closest.append(min_index)

        return closest


    def time_diff(self, other: "FileRecord") -> timedelta:
        """
        Calculate time difference between this and another time range.

        The time difference is 0 if the time overlap. Otherwise it will
        be the maximum of
            - the difference between start of self and end of other
            - the difference between start of other and end of self

        Return:
             A numpy.timedelta64 object representing the time difference.
        """
        time_delta = max(
            self.start - other.end,
            other.start - self.end
        )
        return np.maximum(time_delta, np.timedelta64(0, "s"))
