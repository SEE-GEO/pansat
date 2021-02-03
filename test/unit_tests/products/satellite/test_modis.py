"""
Tests for the pansat.products.satellite.modis module.
"""
from datetime import datetime
import pytest
from pansat.products.satellite.modis import modis_terra_1km, modis_terra_geo

PRODUCTS = [
    modis_terra_1km,
    modis_terra_geo,
]

FILENAMES = {
    str(modis_terra_1km): "MOD021KM.A2020009.0015.061.2020009130917.hdf",
    str(modis_terra_geo): "MOD03.A2008007.0000.061.2017254211539.hdf",
}

DATES = {
    str(modis_terra_1km): datetime(2020, 1, 9, 0, 15),
    str(modis_terra_geo): datetime(2008, 1, 7, 0, 0),
}


@pytest.mark.parametrize("product", PRODUCTS)
def test_filename_regexp(product):
    """
    Ensures that product matches filename.
    """
    filename = FILENAMES[str(product)]
    assert product.matches(filename)


@pytest.mark.parametrize("product", PRODUCTS)
def test_filename_to_date(product):
    """
    Ensures that filename to date yields the right date.
    """
    filename = FILENAMES[str(product)]
    date = product.filename_to_date(filename)
    assert date == DATES[str(product)]


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
@pytest.mark.xfail
def test_download():
    """
    Ensure that downloading of MODIS products works.
    """
    t_0 = datetime(2017, 1, 13, 0, 2)
    t_1 = datetime(2017, 1, 13, 0, 7)
    files = modis_terra_geo.download(t_0, t_1)
    assert len(files) > 0
