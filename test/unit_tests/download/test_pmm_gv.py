"""
Tests for the PMMPVProvider defined in pansat.download.providers.pmm_gv.py
"""
import pytest


from pansat.download.providers.pmm_gv import pmm_gv_provider
from pansat.time import TimeRange
from pansat.products.ground_based.gpm_gv import (
    precip_rate_gpm,
    mask_gpm,
    rqi_gpm,
    gcf_gpm
)


@pytest.mark.parametrize("product", (precip_rate_gpm, mask_gpm, rqi_gpm, gcf_gpm))
def test_provides(product):
    """
    Ensure that provider provides GPM GV products.
    """
    assert pmm_gv_provider.provides(product)

@pytest.mark.parametrize("product", (precip_rate_gpm, mask_gpm, rqi_gpm, gcf_gpm))
def test_find_files(product):
    """
    Ensure that provider provides GPM GV products.
    """
    time_range = TimeRange("2020-07-01T04:22:00")
    recs = pmm_gv_provider.find_files(product, time_range)
    assert len(recs) >= 1
