"""
Tests for NOAA NCEI satellite products.
=======================================
"""
from datetime import datetime
from pathlib import Path

from pansat import FileRecord, TimeRange
from pansat.products.satellite.ncei import (
    gridsat_conus,
    gridsat_goes,
    gridsat_b1,
    ssmi_csu,
    patmosx,
    patmosx_asc,
    patmosx_des,
    isccp_hgm,
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


PATMOSX_FILENAME = "patmosx_v06r00-preliminary_NOAA-15_des_d20230116_c20230125.nc"


def test_patmosx():
    """
    Ensure that PATMOS-X product:
        - Matches filename
        - Correctly parses timestamps
    """
    assert patmosx.matches(PATMOSX_FILENAME)
    assert patmosx_des.matches(PATMOSX_FILENAME)
    assert not patmosx_asc.matches(PATMOSX_FILENAME)

    time_range = patmosx.get_temporal_coverage(PATMOSX_FILENAME)
    assert time_range.start == datetime(2023, 1, 16)

    time_range = patmosx_des.get_temporal_coverage(PATMOSX_FILENAME)
    assert time_range.start == datetime(2023, 1, 16)


ISCCP_FILENAME = "ISCCP-Basic.HGM.v01r00.GLOBAL.1983.07.99.9999.GPC.10KM.CS00.EA1.00.nc"


def test_isccp():
    """
    Ensure that ISCCP HGM product:
        - Matches filename
        - Correctly parses timestamps
    """
    assert isccp_hgm.matches(ISCCP_FILENAME)

    time_range = isccp_hgm.get_temporal_coverage(ISCCP_FILENAME)
    assert time_range.start < datetime(1983, 7, 1)
    assert time_range.end > datetime(1983, 7, 1)
