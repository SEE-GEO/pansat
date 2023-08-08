"""
Test basic product functionality.
"""
from h5py import File


from pansat.products import get_product


def test_get_product():
    """
    Test retrieving a GPM product without prior import.
    """
    product_name = "satellite.gpm.l2b_gpm_cmb"
    l2b_gpm_cmb = get_product(product_name)
    assert l2b_gpm_cmb is not None
