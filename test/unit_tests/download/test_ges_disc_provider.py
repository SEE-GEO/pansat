"""
Test for NASA GES DISC provider.
"""
from copy import copy
from datetime import datetime
import os
import pytest

from conftest import NEEDS_PANSAT_PASSWORD

from pansat.download.providers.ges_disc import (
    ges_disc_provider_day,
    ges_disc_provider_month,
    ges_disc_provider_year,
    ges_disc_provider_merra
)
from pansat.products.satellite.gpm import (
    l1c_r_gpm_gmi,
    l3b_imerg_daily_final,
    l3b_imerg_monthly,
    merged_ir,
)
from pansat.products.reanalysis.merra import (
    m2i3nwasm
)
from pansat.time import TimeRange


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@NEEDS_PANSAT_PASSWORD
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


@NEEDS_PANSAT_PASSWORD
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
    files = ges_disc_provider_month.find_files(l3b_imerg_daily_final, time_range)
    assert len(files) == 1

    start_time = datetime(2019, 12, 31, 0, 0)
    end_time = datetime(2020, 1, 1, 0, 1)
    time_range = TimeRange(start_time, end_time)
    files = ges_disc_provider_month.find_files(l3b_imerg_daily_final, time_range)
    assert len(files) == 2


@NEEDS_PANSAT_PASSWORD
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


@NEEDS_PANSAT_PASSWORD
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


@pytest.mark.slow
def test_ges_disc_provider_merra(tmp_path):
    """
    Test finding and downloading of MERRA data from the GES DISC server.
    """
    m2i3nwasm_v = copy(m2i3nwasm)
    m2i3nwasm_v.variables = ["T"]

    time_range = TimeRange("1980-01-01T12:00:00")
    recs = m2i3nwasm_v.find_files(time_range, provider=ges_disc_provider_merra)
    assert len(recs) == 1

    files = sorted(list(tmp_path.glob("*.nc4")))
    assert len(files) == 0
    recs[0].get(destination=tmp_path)
    files = sorted(list(tmp_path.glob("*.nc4")))
    assert len(files) == 1
