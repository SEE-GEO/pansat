"""
Test for NASA GES DISC provider.
"""
import os
import pytest
from pansat.download.providers.ges_disc import GesdiscProvider
from pansat.products.satellite.gpm import l2a_dpr


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_ges_disc_provider():
    """
    Ensures the GES DISC provider finds files for the GPM DPR L2
    product.
    """
    data_provider = GesdiscProvider(l2a_dpr)
    files = data_provider.get_files_by_day(2018, 10)
    assert files

    n = len(files)
    file = files[n // 2]

    date = l2a_dpr.filename_to_date(file)
    date_file = data_provider.get_file_by_date(date)
    assert date_file == file
