"""
Test for the NCAR Stage IV provider.
"""
from datetime import datetime
from random import randint

import pytest

from pansat import TimeRange
from pansat.products.ground_based.stage4 import surface_precip
from pansat.geometry import LonLatRect


def test_find_provider():
    """
    Ensure that a provider for GridSat GOES is found.
    """
    provider = surface_precip.find_provider()
    assert provider is not None


def test_ncar_stage4_provider():
    """
    Ensure that a single file is found for a sub-monthly time range.
    """
    time_range = TimeRange("2003-01-02", "2003-01-15")
    files = surface_precip.find_files(time_range)
    assert len(files) == 1
