"""
pansat.time
===========

This module provides functionality for dealing with times.
"""
from datetime import datetime
from dataclasses import dataclass
import pandas as pd
import numpy as np


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
    by a given data filee.

    If the temporal extent of a data file cannot be deduced from the
    filename alone or it is not known, the 'end' attribute can be 'None'.
    """

    start: np.datetime64
    end: np.datetime64 = None
