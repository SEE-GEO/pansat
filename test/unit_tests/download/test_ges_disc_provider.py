"""
Test for NASA GES DISC provider.
"""
import os
import pytest
from pansat.download.providers.ges_disc import GesdiscProvider

class MockProduct:
    """
    Mock of the product class providing the product name that is required
    to determine the request URL for the product.
    """
    def __init__(self):
        self.name = "GPM_2A_DPR"


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_ges_disc_provider():
    """ Ensures the GES DISC provider finds files for the GPM DPR L1 product."""
    data_provider = GesdiscProvider(MockProduct())
    files = data_provider.get_files_by_day(2018, 10)
    print(files)
    assert files
