"""
Test for NOAA NCEI provider.
"""
import pytest
from pansat.download.providers.noaa_ncei import NOAANCEIProvider
from pansat.products.satellite.gridsat import gridsat_goes, gridsat_b1


def test_noaa_ncei_provider():
    """
    Ensures the NOAA NCEI provider returns the right number of files per day.
    """
    data_provider = NOAANCEIProvider(gridsat_goes)
    files = data_provider.get_files_by_day(2016, 10)
    assert len(files) == 48

    data_provider = NOAANCEIProvider(gridsat_b1)
    files = data_provider.get_files_by_day(2016, 10)
    assert len(files) == 8
