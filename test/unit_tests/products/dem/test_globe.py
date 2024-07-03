"""
Tests for the pansat.products.dem.globe product
"""

from pansat.products.dem.globe import globe


FILENAMES = ["a10g.gz", "all10.tgz"]


def test_matches():
    """
    Tests that product matches filenames.
    """
    for filename in FILENAMES:
        assert globe.matches(filename)
