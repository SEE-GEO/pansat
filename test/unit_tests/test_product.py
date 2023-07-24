"""
Test basic product functionality.
"""
from h5py import File

from pansat.products.example import (
    hdf5_product
)

def test_product(hdf5_product_data):

    hdf5_product_files = sorted(
        list((hdf5_product_data / "remote").glob("*.h5"))
    )

    rec = FileRecord(
        hdf_product_files[0],
        hdf5_product
    )
