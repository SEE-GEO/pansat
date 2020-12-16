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
    results = open_dap.map_pages(open_dap.extract_gpm_products, "http://spfrnd.de")
    assert not results
