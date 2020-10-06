"""
Tests for provider classes and download functions.

"""


def test_copernicus_provider():
    """
    This test creates an instance for CopernicusProvider and downloads the global air temperatures for a randomly selected data product of the ERA5 reanalysis.
    """

    import pansat.download.providers as provs
    import random
    import datetime
    import os

    product  = random.choice(copernicus_products)

    era = CopernicusProvider(product, 'temperature')
    start = datetime.datetime.today()
    end = datetime.datetime.today()
    dest = product + '.nc'
    era.get_files(start, end, dest)

    assert os.path.exists(dest) == True



