"""
Tests for the pansat.time module
================================
"""
from datetime import datetime, timedelta
import json

import numpy as np

from pansat.time import (
    to_datetime,
    to_datetime64,
    to_timedelta,
    to_timedelta64,
    TimeRange
)


def test_to_datetime():
    """
    Ensure that conversion to timedelta works.
    """
    time = to_datetime(np.datetime64("now"))
    assert isinstance(time, datetime)

    time = to_datetime(datetime.now())
    assert isinstance(time, datetime)

    time = to_datetime("2020-01-01T00:00:00")
    assert isinstance(time, datetime)


def test_to_datetime64():
    """
    Ensure that conversion to datetime64 works.
    """
    d_t = to_datetime64(np.datetime64("now"))
    assert np.issubdtype(d_t, np.datetime64)

    d_t = to_datetime64(datetime.now())
    assert np.issubdtype(d_t, np.datetime64)

    d_t = to_datetime64("2020-01-01T00:00:00")
    assert np.issubdtype(d_t, np.datetime64)


def test_to_timedelta():
    """
    Ensure that conversion to timedelta works.
    """
    d_t = to_timedelta(np.timedelta64(1, "D"))
    assert isinstance(d_t, timedelta)

    d_t = to_timedelta(timedelta(days=1))
    assert isinstance(d_t, timedelta)

    d_t = to_timedelta("1h")
    assert isinstance(d_t, timedelta)


def test_to_timedelta64():
    """
    Ensure that conversion to timedelta64 works.
    """
    d_t = to_timedelta64(np.timedelta64(1, "D"))
    assert np.issubdtype(d_t, np.timedelta64)

    d_t = to_timedelta64(timedelta(days=1))
    assert np.issubdtype(d_t, np.timedelta64)

    d_t = to_timedelta64("1h")
    assert np.issubdtype(d_t, np.timedelta64)


def test_create_time_range():
    """
    Test creation of time range using different time formats.
    """
    start = "2020-01-01T00:00:00"
    end = "2020-01-02T00:00:00"
    time_range_1 = TimeRange(start, end)

    start = datetime(2020, 1, 1)
    end = datetime(2020, 1, 2)
    time_range_2 = TimeRange(start, end)

    assert time_range_1 == time_range_2


def test_time_range_covers():
    """
    Test creation of time range using different time formats.
    """
    start = "2020-01-01T00:00:00"
    end = "2020-01-02T00:00:00"
    time_range = TimeRange(start, end)

    inside = "2020-01-01T00:00:00"
    assert time_range.covers(inside)

    inside = "2020-01-01T12:00:00"
    assert time_range.covers(inside)

    inside = "2020-01-02T00:00:00"
    assert time_range.covers(inside)

    outside = "2020-01-02T00:00:01"
    assert not time_range.covers(outside)


def test_time_range_expand():
    """
    Test expansion of time ranges.
    """
    start = "2020-01-01T00:00:00"
    end = "2020-01-02T00:00:00"
    time_range = TimeRange(start, end)

    left = np.datetime64("2019-12-31T23:00:00")
    assert not time_range.covers(left)
    right = np.datetime64("2020-01-02T01:00:00")
    assert not time_range.covers(right)
    new_range = time_range.expand(timedelta(hours=1))
    assert new_range.covers(left)
    assert new_range.covers(right)
    new_range = time_range.expand((timedelta(hours=1), timedelta(hours=0)))
    assert new_range.covers(left)
    assert not new_range.covers(right)


def test_json_serialization():
    """
    Test json serialization of time range objects.

    """
    start = "2020-01-01T00:00:00"
    end = "2020-01-02T00:00:00"
    time_range = TimeRange(start, end)

    json_repr = time_range.to_json()

    def object_hook(dct):
        if "TimeRange" in dct:
            return TimeRange.from_dict(dct["TimeRange"])
        return dct

    time_range_loaded = json.loads(json_repr, object_hook=object_hook)

    assert time_range == time_range_loaded


def test_find_closest():
    """
    Test finding of closest time range.
    """
    time_range = TimeRange("2020-01-01T01:00:00", "2020-01-01T02:00:00")

    other = [
        TimeRange("2020-01-01T00:00:00", "2020-01-01T00:30:00"),
        TimeRange("2020-01-01T00:45:00", "2020-01-01T01:15:00"),
        TimeRange("2020-01-01T02:01:00", "2020-01-01T02:04:00"),
    ]

    closest = time_range.find_closest(other)
    assert len(closest) == 1
    assert closest[0] == other[1]

    other = [
        TimeRange("2020-01-01T01:00:00", "2020-01-01T01:30:00"),
        TimeRange("2020-01-01T00:45:00", "2020-01-01T01:15:00"),
        TimeRange("2020-01-01T01:45:00", "2020-01-01T02:04:00"),
    ]
    closest = time_range.find_closest(other)
    assert len(closest) == 3

    other = [
        TimeRange("2020-01-01T00:00:00", "2020-01-01T00:45:00"),
        TimeRange("2020-01-01T02:30:00", "2020-01-01T03:00:00"),
    ]
    closest = time_range.find_closest(other)
    assert len(closest) == 1
    assert closest[0] == other[0]

    other = [
        TimeRange("2020-01-01T00:00:00", "2020-01-01T00:45:00"),
        TimeRange("2020-01-01T02:01:00", "2020-01-01T03:00:00"),
    ]
    closest = time_range.find_closest(other)
    assert len(closest) == 1
    assert closest[0] == other[1]


def test_find_closest_ind():
    """
    Test finding of closest time range by index.
    """
    time_range = TimeRange("2020-01-01T01:00:00", "2020-01-01T02:00:00")

    other = [
        TimeRange("2020-01-01T00:00:00", "2020-01-01T00:30:00"),
        TimeRange("2020-01-01T00:45:00", "2020-01-01T01:15:00"),
        TimeRange("2020-01-01T02:01:00", "2020-01-01T02:04:00"),
    ]

    closest = time_range.find_closest_ind(other)
    assert len(closest) == 1
    assert closest[0] == 1

    other = [
        TimeRange("2020-01-01T01:00:00", "2020-01-01T01:30:00"),
        TimeRange("2020-01-01T00:45:00", "2020-01-01T01:15:00"),
        TimeRange("2020-01-01T01:45:00", "2020-01-01T02:04:00"),
    ]
    closest = time_range.find_closest_ind(other)
    assert len(closest) == 3

    other = [
        TimeRange("2020-01-01T00:00:00", "2020-01-01T00:45:00"),
        TimeRange("2020-01-01T02:30:00", "2020-01-01T03:00:00"),
    ]
    closest = time_range.find_closest_ind(other)
    assert len(closest) == 1
    assert closest[0] == 0

    other = [
        TimeRange("2020-01-01T00:00:00", "2020-01-01T00:45:00"),
        TimeRange("2020-01-01T02:01:00", "2020-01-01T03:00:00"),
    ]
    closest = time_range.find_closest_ind(other)
    assert len(closest) == 1
    assert closest[0] == 1


def test_time_diff():
    """
    Ensure that 'time_diff' method

        - returns 0, when time intervals overlap.
        - returns a positive number when they don't.
    """
    time_range = TimeRange("2020-01-01T01:00:00", "2020-01-01T02:00:00")
    other = [
        TimeRange("2020-01-01T00:00:00", "2020-01-01T00:30:00"),
        TimeRange("2020-01-01T00:45:00", "2020-01-01T01:15:00"),
        TimeRange("2020-01-01T02:01:00", "2020-01-01T02:04:00"),
    ]
    assert time_range.time_diff(other[0]) > np.timedelta64(0, "s")
    assert time_range.time_diff(other[1]) == np.timedelta64(0, "s")
    assert time_range.time_diff(other[2]) > np.timedelta64(0, "s")


def test_add():
    """
    Test adding of time differences to time range.
    """
    time_range = TimeRange("2020-01-01T01:00:00", "2020-01-01T02:00:00")
    assert not time_range.covers("2020-01-01T02:30:00")

    time_range = time_range + np.timedelta64(30, "m")
    assert time_range.covers("2020-01-01T02:30:00")


def test_sub():
    """
    Test subtracting of time differences from a time range.
    """
    time_range = TimeRange("2020-01-01T01:00:00", "2020-01-01T02:00:00")
    assert not time_range.covers("2019-01-01T00:30:00")

    time_range = time_range - np.timedelta64(30, "m")
    assert time_range.covers("2020-01-01T00:45:00")
