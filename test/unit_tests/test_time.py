"""
Tests for the pansat.time module
================================
"""
from datetime import datetime

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
