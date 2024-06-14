"""
Tests for the pansat.products.reanalysis.merra module.
"""
from datetime import datetime

from conftest import NEEDS_PANSAT_PASSWORD

from pansat.time import TimeRange
from pansat.products.reanalysis.merra import (
    m2i3nwasm,
    m2conxasm
)


def test_filename_regexp():
    """
    Test that filename regexp matches actual MERRA2 filename.
    """
    filename = "MERRA2_100.inst3_3d_asm_Nv.19800101.nc4"
    assert m2i3nwasm.matches(filename)


def test_get_temporal_coverage():
    """
    Test determining the temporal coverage of a given record.
    """
    filename = "MERRA2_100.inst3_3d_asm_Nv.19800101.nc4"
    time_range = m2i3nwasm.get_temporal_coverage(filename)
    assert time_range.start == datetime(1980, 1, 1)
    assert time_range.end == datetime(1980, 1, 2)


def test_find_files():
    """
    Test finding of MERRA2 records.
    """
    time_range = TimeRange("1980-01-01T12:00:00")
    recs = m2i3nwasm.find_files(time_range)
    assert len(recs) == 1


def test_find_files_constant():
    """
    Test finding of MERRA2 records.
    """
    time_range = TimeRange(
        "1980-01-01T12:00:00",
        "1981-01-01T12:00:00",
    )
    recs = m2conxasm.find_files(time_range)
    assert len(recs) == 1
