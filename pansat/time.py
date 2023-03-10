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
        raise ValueError(
            "Could not convert '%s' to datetime object."
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
        raise ValueError(
            "Could not convert '%s' to datetime object."
        )
