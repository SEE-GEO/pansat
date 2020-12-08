"""
Test for NASA GES DISC provider.
"""
import datetime
import os
import pytest
from pansat.products.ground_based.opera import rainfall_rate
from pansat.download.providers.meteo_france import GeoservicesProvider

HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_meteofrance_provider():
    """
    Ensures the GES DISC provider finds files for the GPM DPR L2
    product.
    """
    data_provider = GeoservicesProvider(rainfall_rate)
    start_time = datetime.datetime(2018, 10, 10, 10)
    end_time = datetime.datetime(2018, 10, 10, 10, 15)
    files = data_provider.download(start_time, end_time, ".")
    assert files
