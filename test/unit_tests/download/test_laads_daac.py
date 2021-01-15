from pansat.download.providers import LAADSDAACProvider
from pansat.products.satellite.modis import modis_terra_1km
from datetime import datetime


def test_get_files_by_day():
    provider = LAADSDAACProvider(modis_terra_1km)
    assert len(provider.get_files_by_day(2018, 1)) > 1
