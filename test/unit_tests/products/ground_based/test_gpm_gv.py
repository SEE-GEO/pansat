"""
Tests for the gpm.gv module.
"""
from datetime import datetime
import pytest

from pansat.products.ground_based.gpm_gv import (
    precip_rate_gpm,
    mask_gpm,
    rqi_gpm,
    gcf_gpm
)


FILES = {
    "ground_based.gpm_gv.precip_rate_gpm": (
        "PRECIPRATE.GC.20200701.042200.36021.dat.gz",
        datetime(2020, 7, 1, 4, 22)
    ),
    "ground_based.gpm_gv.mask_gpm": (
        "MASK.20200701.042200.36021.dat.gz",
        datetime(2020, 7, 1, 4, 22)
    ),
    "ground_based.gpm_gv.rqi_gpm": (
        "RQI.20200701.042200.36021.dat.gz",
        datetime(2020, 7, 1, 4, 22)
    ),
    "ground_based.gpm_gv.1hcf_gpm": (
        "1HCF.20200701.042200.36021.dat.gz",
        datetime(2020, 7, 1, 4, 22)
    ),
}


@pytest.mark.parametrize("product", (precip_rate_gpm, mask_gpm, rqi_gpm, gcf_gpm))
def test_filename_matches(product):
    """
    Ensure that products match corresponding filenames.
    """
    fname, _ = FILES[product.name]
    assert product.matches(fname)


@pytest.mark.parametrize("product", (precip_rate_gpm, mask_gpm, rqi_gpm, gcf_gpm))
def test_temporal_coverage(product):
    """
    Ensure that calculated temporal coverage matches expected values.
    """
    fname, start = FILES[product.name]
    time_range = product.get_temporal_coverage(fname)
    assert time_range.start == start
