"""
Tests for the GPM product.
"""
from datetime import datetime
import os
import pytest
import pansat.products.satellite.gpm as gpm

PRODUCTS = [gpm.l2a_dpr]
TEST_NAMES = {
    str(gpm.l1c_metopb_mhs): (
        "1C.METOPB.MHS.XCAL2016-V.20161124" "-S111934-E130055.021719.V05A.HDF5"
    ),
    str(gpm.l1c_gpm_gmi_r): (
        "1C-R.GPM.GMI.XCAL2016-C.20161121-S110157" "-E123429.015524.V05A.HDF5"
    ),
    str(gpm.l2a_dpr): (
        "2A.GPM.DPR.V8-20180723.20180110-S102756-E120031" ".021979.V06A.HDF5"
    ),
    str(gpm.l2a_gprof_gpm_gmi): (
        "2A.GPM.GMI.GPROF2017v1.20161124-S113145" "-E130417.015571.V05A.HDF"
    ),
    str(gpm.l2a_gprof_metopb_mhs): (
        "2A.METOPB.MHS.GPROF2017v2.20161121" "-S104106-E122227.021676.V05C.HDF"
    ),
}
TEST_TIMES = {
    str(gpm.l1c_metopb_mhs): datetime(2016, 11, 24, 11, 19, 34),
    str(gpm.l1c_gpm_gmi_r): datetime(2016, 11, 21, 11, 1, 57),
    str(gpm.l2a_dpr): datetime(2018, 1, 10, 10, 27, 56),
    str(gpm.l2a_gprof_gpm_gmi): datetime(2016, 11, 24, 11, 31, 45),
    str(gpm.l2a_gprof_metopb_mhs): datetime(2016, 11, 21, 10, 41, 6),
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
@pytest.mark.usefixtures("test_identities")
def test_download():
    """
    Download CloudSat L1B file.
    """
    product = gpm.l2a_gprof_metopb_mhs
    t_0 = datetime(2018, 6, 1, 10)
    t_1 = datetime(2018, 6, 1, 12)
    product.download(t_0, t_1)
