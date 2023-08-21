from pansat.download.providers.cloudnet import CloudnetProvider
from pansat.products.ground_based.cloudnet import CloudnetProduct, l1_radar, l2_iwc

import xarray as xr


def test_download(tmpdir):
    """
    Test discovery and of Cloudnet files.
    """
    product = CloudnetProduct("iwc", "", "palaiseau")
    provider = CloudnetProvider(product)
    files = provider.get_files_by_day(2020, 1)
    assert len(files) == 1

    product = CloudnetProduct("iwc", "")
    provider = CloudnetProvider(product)
    files = provider.get_files_by_day(2020, 1)
    assert len(files) == 9


def test_filenames():
    """
    Ensures that the matching of filenames works.
    """
    radar_files = ["20230503_palaiseau_basta.nc", "20230503_norunda_rpg-fmcw-94.nc"]
    for filename in radar_files:
        assert l1_radar.matches(filename)

    product = CloudnetProduct("iwc", "", "palaiseau")
    assert product.matches(radar_files[0])
    assert not product.matches(radar_files[1])

    iwc_files = ["20230503_norunda_iwc-Z-T-method.nc"]
    for filename in iwc_files:
        assert l2_iwc.matches(filename)
