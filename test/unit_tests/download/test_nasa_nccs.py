"""
Tests for the pansat.download.providers.nasa_nccs module.
"""

from datetime import datetime, timedelta

import pytest

from pansat.products.model.geos import (
    inst3_3d_asm_nv,
    inst3_2d_asm_nx,
    tavg1_2d_lnd_nx,
    tavg1_2d_flx_nx,
    tavg1_2d_rad_nx,
    inst3_3d_asm_nv_fc,
    tavg1_2d_lnd_nx_fc,
    tavg1_2d_flx_nx_fc,
    tavg1_2d_rad_nx_fc,
)


@pytest.mark.parametrize(
    "product",
    [
        inst3_3d_asm_nv,
        inst3_2d_asm_nx,
        tavg1_2d_lnd_nx,
        tavg1_2d_flx_nx,
        tavg1_2d_rad_nx,
        inst3_3d_asm_nv_fc,
        tavg1_2d_lnd_nx_fc,
        tavg1_2d_flx_nx_fc,
        tavg1_2d_rad_nx_fc,
    ],
)
def test_find_files(product):
    """
    Ensure that data provider finds files

    """
    date = datetime.now() - timedelta(days=2)
    files = product.find_files(date)
    assert len(files) > 0
