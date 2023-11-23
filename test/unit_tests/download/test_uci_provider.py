"""
Tests for the UciProvider providing files from persiann.eng.uci.edu provider.
"""
from datetime import datetime
from pansat.time import TimeRange
from pansat.download.providers.uci import uci_provider
from pansat.products.satellite.persiann import (
    cdr_daily,
    cdr_monthly,
    cdr_yearly
)

def test_find_files():
    """
    Ensure that the UCIProvider finds the expected number of files for a
    range of PERSIANN product with different temporal resolutions.
    """
    time_range = TimeRange("2020-01-01T01:00:00", "2020-01-02T02:00:00")
    files = uci_provider.find_files(cdr_daily, time_range)
    assert len(files) == 2

    time_range = TimeRange("2020-01-01T01:00:00", "2020-01-02T02:00:00")
    files = uci_provider.find_files(cdr_monthly, time_range)
    assert len(files) == 1

    time_range = TimeRange("2020-01-01T01:00:00", "2020-01-02T02:00:00")
    files = uci_provider.find_files(cdr_yearly, time_range)
    assert len(files) == 1
