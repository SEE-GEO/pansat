"""
Tests for scrapers for OpenDAP servers.
"""
from pansat.download.providers.scrapers import open_dap


def test_retrieve_page():
    """
    Ensure that retrieving an existing URL returns non-empty content.
    """
    response = open_dap.retrieve_page("http://spfrnd.de")
    assert response.text != ""


def test_is_date():
    """
    Ensure that discovery of directories ending on dates works as
    expected.
    """
    assert not open_dap.is_date("/foo/bar/")
    assert open_dap.is_date("/foo/bar/300/")
    assert open_dap.is_date("/foo/bar/300/001")


def test_map_pages():
    """
    Assert that 1 GPM product is found when listing of product for specific
    day is parsed.


    """
    url = (
        "https://gpm1.gesdisc.eosdis.nasa.gov/opendap/GPM_L1C/"
        "GPM_1CF10SSMI.07/1990/344"
    )
    results = open_dap.map_pages(open_dap.extract_gpm_products, url)
    assert len(results) == 1

    url = "https://gpm1.gesdisc.eosdis.nasa.gov/opendap/GPM_L1C/" "GPM_1CF10SSMI.06"
    open_dap.map_pages(open_dap.extract_gpm_products, url)


def test_extract_gpm_products():
    """
    Assert that GPM products is correctly extracted from HTML page.
    """
    url = (
        "https://gpm1.gesdisc.eosdis.nasa.gov/opendap/GPM_L1C/"
        "GPM_1CGPMGMI_R.07/2014/063/contents.html"
    )
    response = open_dap.retrieve_page(url)
    results = open_dap.extract_gpm_products(response.text)

    assert results == ("1C", "R", "GPM", "GMI")
