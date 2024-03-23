"""
Tests for the pansat.products.satellite.ccic module.
"""
from datetime import datetime

import pytest

from pansat.products.satellite.ccic import (
    ccic_gridsat,
    ccic_cpcir
)

CCIC_FILES = {
    "satellite.ccic.ccic_cpcir": (
        "ccic_cpcir_20100101000000.zarr",
        datetime(2010, 1, 1, 0, 0, 0)
    ),
    "satellite.ccic.ccic_gridsat": (
        "ccic_gridsat_20100101000000.zarr",
        datetime(2010, 1, 1, 0, 0, 0)
    ),
}


@pytest.mark.parametrize("product", [ccic_cpcir, ccic_gridsat])
def test_ccic_product(product):
    """
    Assert that filename match product and that extracted temporal coverage
    covers the given time.
    """
    fname, date = CCIC_FILES[product.name]
    assert product.matches(fname)
    time_range = product.get_temporal_coverage(fname)
    assert time_range.covers(date)
