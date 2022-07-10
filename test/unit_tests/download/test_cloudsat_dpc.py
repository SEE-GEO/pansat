"""
Test for CloudSat DPC provider.
"""
import os
import pytest
from pansat.download.providers.cloudsat_dpc import CloudSatDPCProvider
from pansat.products.satellite.cloud_sat import l2c_ice


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_cloudsat_dpc_provider():
    """
    Ensures the GES DISC provider finds files for the CloudSat 2C-Ice product.
    """
    data_provider = CloudSatDPCProvider(l2c_ice)
    files = data_provider.get_files_by_day(2016, 10)
    assert len(files) == 15

    filename = files[0]
    date = l2c_ice.filename_to_date(filename)
    assert date.day == 10
