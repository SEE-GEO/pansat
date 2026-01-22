"""
Tests for the GPM product.
"""
from datetime import datetime
import os

import pytest

from pansat import FileRecord
import pansat.products.satellite.gpm as gpm
from pansat.time import TimeRange

HAS_HDF = False
try:
    from pansat.formats.hdf5 import HDF5File

    HAS_HDF = True
except Exception:
    pass

PRODUCTS = [gpm.l1c_metopb_mhs, gpm.l1c_r_gpm_gmi, gpm.l2a_gpm_dpr, gpm.l2b_gpm_cmb]
TEST_NAMES = {
    str(gpm.l1c_metopb_mhs): (
        "1C.METOPB.MHS.XCAL2016-V.20140103-S020041-E034201.006711.V07A.HDF5"
    ),
    str(gpm.l1c_r_gpm_gmi): (
        "1C-R.GPM.GMI.XCAL2016-C.20180105-S022251-E035525.021896.V07A.HDF5"
    ),
    str(gpm.l2a_gpm_dpr): (
        "2A.GPM.DPR.V9-20211125.20230108-S031503-E044734.050349.V07A.HDF5"
    ),
    str(gpm.l2b_gpm_cmb): (
        "2B.GPM.DPRGMI.CORRA2022.20161124-S113145-E130417.015571.V07A.HDF5"
    ),
}
TEST_TIMES = {
    str(gpm.l1c_metopb_mhs): datetime(2014, 1, 3, 2, 0, 41),
    str(gpm.l1c_r_gpm_gmi): datetime(2018, 1, 5, 2, 22, 51),
    str(gpm.l2a_gpm_dpr): datetime(2023, 1, 8, 3, 15, 3),
    str(gpm.l2b_gpm_cmb): datetime(2016, 11, 24, 11, 31, 45),
}

HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.parametrize("product", PRODUCTS)
def test_matches(product):
    """
    Assert that matches method returns true on the filename.
    """
    filename = TEST_NAMES[str(product)]
    assert product.matches(filename)


@pytest.mark.parametrize("product", PRODUCTS)
def test_filename_to_date(product):
    """
    Assert that matches method returns true on the filename.
    """
    filename = TEST_NAMES[str(product)]
    reference_time = TEST_TIMES[str(product)]
    time = product.filename_to_date(filename)
    assert time == reference_time


ITE_PRODUCTS = [gpm.l1c_tropics03_tms, gpm.l1c_tropics06_tms]
ITE_NAMES = {
    str(gpm.l1c_tropics03_tms): (
        "1C.TROPICS03.TMS.XCAL2023-N.20230701-S000811-E014333.000542.PAR702.HDF5"
    ),
    str(gpm.l1c_tropics06_tms): (
        "1C.TROPICS06.TMS.XCAL2023-N.20230701-S012854-E030411.000817.PAR702.HDF5"
    ),
}
ITE_TIMES = {
    str(gpm.l1c_tropics03_tms): datetime(2023, 7, 1, 0, 8, 11),
    str(gpm.l1c_tropics06_tms): datetime(2023, 7, 1, 1, 28, 54),
}


@pytest.mark.parametrize("product", ITE_PRODUCTS)
def test_ite_products(product):
    """
    Tests of ITE products.
    """
    filename = ITE_NAMES[str(product)]
    assert product.matches(filename)

    rec = FileRecord(filename)
    time_range = product.get_temporal_coverage(rec)
    assert time_range.start == ITE_TIMES[str(product)]


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.skipif(not HAS_HDF, reason="h5py not available.")
@pytest.mark.slow
def test_download(tmp_path):
    """
    Download l2a_gprof_metopb_mhs file
    """
    product = gpm.l2a_gprof_metopb_mhs
    time_range = TimeRange(
        "2023-06-01T10:00:00",
        "2023-06-01T11:00:00",
    )
    files = product.download(time_range, destination=tmp_path)
    dataset = gpm.l2a_gprof_metopb_mhs.open(files[0])
    dates = dataset["scan_time"].data
    start_date = datetime.utcfromtimestamp(dates[0].astype(int) * 1e-9)
    assert start_date.year == 2023
    assert start_date.month == 6
    assert start_date.day == 1


def test_prps_product():

    fname = "TROPICS03.PRPS.L2B.Orbit03339.V04-02.ST20240101-122806.ET20240101-140311.CT20241002-143542.nc"

    assert gpm.l2b_prps_tropics03_tms_v0402.matches(fname)
    assert gpm.l2b_prps_tropics03_tms_v0402.name == "satellite.gpm.l2b_prps_tropics03_tms_v0402"

    time_range = gpm.l2b_prps_tropics03_tms_v0402.get_temporal_coverage(fname)
    assert time_range.start == datetime(2024, 1, 1, 12, 28, 6)
    assert time_range.end == datetime(2024, 1, 1, 14, 3, 11)
