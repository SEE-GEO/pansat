"""
Tests for NOAA NCEI satellite products.
=======================================
"""
from datetime import datetime
from pathlib import Path

from pansat import FileRecord
from pansat.products.satellite.ncei import (
    gridsat_conus,
    gridsat_goes,
    gridsat_b1,
    ssmi_csu
)

CONUS_FILENAME = "GridSat-CONUS.goes08.1994.09.01.0000.v01.nc"


def test_gridsat_conus():
    """
    Ensure that gridsat_conus product:
       - Matches filename
       - Correctly parses timestamps
    """
    path = Path(CONUS_FILENAME)
    rec = FileRecord(
        local_path=path,
    )
    assert gridsat_conus.matches(CONUS_FILENAME)
    time_range = gridsat_conus.get_temporal_coverage(rec)
    assert time_range.covers(datetime(1994, 9, 1, 0))


GOES_FILENAME = "GridSat-GOES.goes08.1994.09.01.0000.v01.nc"


def test_gridsat_goes():
    """
    Ensure that gridsat_goes product:
       - Matches filename
       - Correctly parses timestamps
    """
    path = Path(GOES_FILENAME)
    rec = FileRecord(
        local_path=path,
    )
    assert gridsat_goes.matches(GOES_FILENAME)
    time_range = gridsat_goes.get_temporal_coverage(rec)
    assert time_range.covers(datetime(1994, 9, 1, 0))


GRIDSAT_B1_FILENAME = "GRIDSAT-B1.1980.01.01.00.v02r01.nc"


def test_gridsat_b1():
    """
    Ensure that gridsat_b1 product:
       - Matches filename
       - Correctly parses timestamps
    """
    path = Path(GRIDSAT_B1_FILENAME)
    rec = FileRecord(
        local_path=path,
    )
    assert gridsat_b1.matches(rec)
    time_range = gridsat_b1.get_temporal_coverage(rec)
    assert time_range.covers(datetime(1980, 1, 1, 0))


SSMI_CSU_FILENAME = "CSU_SSMI_FCDR_V02R00_F08_D19870709_S0126_E0308_R000268.nc"


def test_ssmi_csu():
    """
    Ensure that ssmi_csu product:
       - Matches filename
       - Correctly parses timestamps
    """
    path = Path(SSMI_CSU_FILENAME)
    rec = FileRecord(
        local_path=path,
    )
    assert ssmi_csu.matches(rec)

    time_range = ssmi_csu.get_temporal_coverage(rec)
    assert time_range.covers(datetime(1987, 7, 9, 2))
