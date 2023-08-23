"""
Test for NASA GES DISC provider.
"""
from datetime import datetime
import os
import pytest

from pansat.download.providers.ges_disc import (
    ges_disc_provider_day,
    ges_disc_provider_month,
    ges_disc_provider_year
)
from pansat.products.satellite.gpm import (
    l1c_r_gpm_gmi,
    l3b_imerg_daily_final,
    l3b_imerg_monthly,
    merged_ir
)
from pansat.time import TimeRange


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_ges_disc_provider_day():
    """
    Ensures the GES DISC provider finds files for the GPM DPR L2
    product.
    """
    assert ges_disc_provider_day.provides(l1c_r_gpm_gmi)
    assert not ges_disc_provider_day.provides(l3b_imerg_daily_final)
    assert not ges_disc_provider_day.provides(l3b_imerg_monthly)

    start_time = datetime(2020, 1, 1, 0, 0)
    time_range = TimeRange(start_time, start_time)
    files = ges_disc_provider_day.find_files(l1c_r_gpm_gmi, time_range)
    assert len(files) == 1

@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_ges_disc_provider_month():
    """
    Ensures the GES DISC provider finds files for the GPM DPR L2
    product.
    """
    assert not ges_disc_provider_month.provides(l1c_r_gpm_gmi)
    assert ges_disc_provider_month.provides(l3b_imerg_daily_final)
    assert not ges_disc_provider_month.provides(l3b_imerg_monthly)

    start_time = datetime(2020, 1, 1, 0, 0)
    time_range = TimeRange(start_time, start_time)
    files = ges_disc_provider_month.find_files(
        l3b_imerg_daily_final,
        time_range
    )
    assert len(files) == 1

    start_time = datetime(2019, 12, 31, 0, 0)
    end_time = datetime(2020, 1, 1, 0, 1)
    time_range = TimeRange(start_time, end_time)
    files = ges_disc_provider_month.find_files(
        l3b_imerg_daily_final,
        time_range
    )
    assert len(files) == 2

@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_ges_disc_provider_year():
    """
    Ensures the GES DISC provider finds files for the GPM DPR L2
    product.
    """
    assert not ges_disc_provider_year.provides(l1c_r_gpm_gmi)
    assert not ges_disc_provider_year.provides(l3b_imerg_daily_final)
    assert ges_disc_provider_year.provides(l3b_imerg_monthly)

    start_time = datetime(2020, 1, 1, 0, 0)
    time_range = TimeRange(start_time, start_time)
    files = ges_disc_provider_year.find_files(l3b_imerg_monthly, time_range)
    assert len(files) == 1

    start_time = datetime(2019, 12, 31, 0, 0)
    end_time = datetime(2020, 1, 1, 0, 1)
    time_range = TimeRange(start_time, end_time)
    files = ges_disc_provider_year.find_files(l3b_imerg_monthly, time_range)
    assert len(files) == 2


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_ges_disc_provider_merged_ir():
    """
    Ensures the GES DISC provider finds files for the GPM DPR L2
    product.
    """
    assert ges_disc_provider_day.provides(merged_ir)
    assert not ges_disc_provider_month.provides(merged_ir)
    assert not ges_disc_provider_year.provides(merged_ir)

    start_time = datetime(2020, 1, 1, 0, 1)
    time_range = TimeRange(start_time, start_time)
    files = ges_disc_provider_day.find_files(merged_ir, time_range)
    assert len(files) == 1

    start_time = datetime(2019, 12, 31, 23, 30)
    end_time = datetime(2020, 1, 1, 0, 1)
    time_range = TimeRange(start_time, end_time)
    files = ges_disc_provider_day.find_files(merged_ir, time_range)
    assert len(files) == 2
