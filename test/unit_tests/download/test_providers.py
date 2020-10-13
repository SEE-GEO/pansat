"""
Tests for provider classes and download functions.

"""


def test_copernicus_provider(tmpdir):
    """
    This test creates an instance for CopernicusProvider class and downloads global air temperatures for a randomly selected data product among available ERA5 reanalysis datasets.
    """

    import pansat.download.providers as provs
    import random
    import datetime
    import os

    product  = random.choice(provs.copernicus_products)
    variable = '2m_temperature'
    if 'pressure' in product:
        variable = 'temperature'
    era = provs.CopernicusProvider(product, variable)

    start = datetime.datetime(2000, 1, 1, 10)
    end = datetime.datetime(2000, 1, 1, 11)
    dest = tmpdir

    era.download(start, end, dest)

    assert dest.exists()





