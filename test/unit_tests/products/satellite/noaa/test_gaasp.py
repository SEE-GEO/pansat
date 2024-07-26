"""
Tests for the pansat.products.satellite.noaa.gaasp module.
"""
from datetime import datetime
import os
from pathlib import Path

import numpy as np
import pytest

from pansat.catalog import Index
from pansat.products.satellite.noaa.gaasp import l1b_gcomw1_amsr2


NOAA_GAASP_DATA = os.environ.get("NOAA_GAASP_DATA")
NEEDS_GAASP_DATA = pytest.mark.skipif(
    NOAA_GAASP_DATA is None,
    reason="Needs GAASP test data"
)


def test_match_filename():
    """
    Ensure that filename regexp matches actual filename.
    """
    fname = "GAASP-L1B_v2r2_GW1_s202112311612140_e202112311751120_c202112311858050.h5"
    assert l1b_gcomw1_amsr2.matches(fname)


def test_get_temporal_coverage():
    """
    Ensure that filename regexp matches actual filename.
    """
    fname = "GAASP-L1B_v2r2_GW1_s202112311612140_e202112311751120_c202112311858050.h5"
    time_range = l1b_gcomw1_amsr2.get_temporal_coverage(fname)
    assert time_range.start == datetime(2021, 12, 31, 16, 12, 14)
    assert time_range.end == datetime(2021, 12, 31, 17, 51, 12)


@NEEDS_GAASP_DATA
def test_open():
    """
    Test opening of AMSR2 files and ensure that all loaded brightness temperatures
    are physical.
    """
    files = sorted(list(Path(NOAA_GAASP_DATA).glob("*.h5")))
    data = l1b_gcomw1_amsr2.open(files[0])
    assert "tbs_s1" in data
    assert "tbs_s2" in data
    assert "tbs_s3" in data

    assert np.nanmax(data["tbs_s1"].data) < 400
    assert np.nanmin(data["tbs_s1"].data) > 0
    assert np.nanmax(data["tbs_s2"].data) < 400
    assert np.nanmin(data["tbs_s2"].data) > 0
    assert np.nanmax(data["tbs_s3"].data) < 400
    assert np.nanmin(data["tbs_s3"].data) > 0


@NEEDS_GAASP_DATA
def test_indexing():
    """
    Test indexing of AMSR2 files.
    """
    files = sorted(list(Path(NOAA_GAASP_DATA).glob("*.h5")))
    assert len(files) > 0
    index = Index.index(l1b_gcomw1_amsr2, files)
    assert len(index) > 0
