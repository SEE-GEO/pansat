"""
Tests for the GPM product.
"""
from datetime import datetime
import os
import pytest
import pansat.products.satellite.gpm as gpm

HAS_HDF = False
try:
    from pansat.formats.hdf5 import HDF5File

    HAS_HDF = True
except Exception:
    pass

PRODUCTS = [gpm.l2a_gpm_dpr]
TEST_NAMES = {
    str(gpm.l1c_metopb_mhs): (
        "1C.METOPB.MHS.XCAL2016-V.20140103-S020041-E034201.006711.V07A.HDF5"
    ),
    str(gpm.l1c_r_gpm_gmi): (
        "1C.GPM.GMI.XCAL2016-C.20180105-S022251-E035525.021896.V07A.HDF5"
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


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.skipif(not HAS_HDF, reason="h5py not available.")
@pytest.mark.xfail
def test_download(tmp_path):
    """
    Download l2a_gprof_metopb_mhs file
    """
    product = gpm.l2a_gprof_metopb_mhs
    t_0 = datetime(2018, 6, 1, 10)
    t_1 = datetime(2018, 6, 1, 11)

    dest = gpm.l2a_gprof_metopb_mhs.default_destination
    files = product.download(t_0, t_1, tmp_path / dest)
    dataset = gpm.l2a_gprof_metopb_mhs.open(files[0])
    dates = dataset["scan_time"].data
    start_date = datetime.utcfromtimestamp(dates[0].astype(int) * 1e-9)
    assert start_date.year == 2018
    assert start_date.month == 6
    assert start_date.day == 1
