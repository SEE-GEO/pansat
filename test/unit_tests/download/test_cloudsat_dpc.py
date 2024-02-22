"""
Test for CloudSat DPC provider.
"""
from datetime import datetime
import os
import pytest

from pansat.download.providers.cloudsat_dpc import CloudSatDPCProvider
from pansat.products.satellite.cloudsat import l2c_ice


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.skip("Server cannot be reached from arbitrary IPs.")
@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_cloudsat_dpc_provider():
    """
    Ensures the GES DISC provider finds files for the CloudSat 2C-Ice product.
    """
    data_provider = CloudSatDPCProvider()
    date = datetime(2016, 10, 1)
    files = data_provider.find_files_by_day(l2c_ice, date)
    assert len(files) == 15

    filename = files[0]
    time_range = l2c_ice.get_temporal_coverage(filename)
    assert time_range.start.month == 10
    assert time_range.start.day == 1
