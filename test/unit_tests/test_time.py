"""
Tests for the pansat.time module
================================
"""
from datetime import datetime, timedelta
import json

import numpy as np

from pansat.time import TimeRange


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
    new_range = time_range.expand(
        timedelta(hours=1)
    )
    assert new_range.covers(left)
    assert new_range.covers(right)
    new_range = time_range.expand(
        (timedelta(hours=1), timedelta(hours=0))
    )
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

    time_range_loaded = json.loads(
        json_repr,
        object_hook=object_hook
    )

    assert time_range == time_range_loaded
