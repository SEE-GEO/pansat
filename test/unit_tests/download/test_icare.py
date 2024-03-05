"""
Tests for the download.providers.icare provider.
================================================
"""
from conftest import NEEDS_PANSAT_PASSWORD
import pytest

from pansat import TimeRange
from pansat.products.satellite.cloudsat import l1b_cpr
from pansat.download.providers.icare import icare_provider


@NEEDS_PANSAT_PASSWORD
def test_find_files():
    recs = l1b_cpr.find_files(
        TimeRange("2012-01-01T00:00:00", "2012-01-01T01:00:00"),
        provider=icare_provider
    )
    assert len(recs) == 1


@pytest.mark.slow
@NEEDS_PANSAT_PASSWORD
def test_download_file(tmp_path):
    recs = l1b_cpr.find_files(
        TimeRange("2012-01-01T00:00:00", "2012-01-01T01:00:00"),
        provider=icare_provider
    )
    rec = recs[0].get(destination=tmp_path)
    assert rec.local_path.exists()
