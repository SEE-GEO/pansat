from pathlib import Path
from pansat.products.product_description import ProductDescription

TEST_DATA = Path(__file__).parent / "data" / "test_description.ini"
TEST_FILE_HDF = Path(__file__).parent / "data" / "test_file.hdf"

HAS_HDF = False
try:
    import pyhdf
    from pansat.formats.hdf4 import HDF4File

    HAS_HDF = True
except Exception:
    pass


def test_read_product_description():
    """
    Reads product description test file and checks that the description
    attributes are parsed correctly.
    """
    description = ProductDescription(TEST_DATA)

    assert description.name == "test-description"

    assert len(description.dimensions) == 2
    assert description.dimensions[0].name == "dimension_1"
    assert description.dimensions[1].name == "dimension_2"

    assert len(description.coordinates) == 2
    assert description.coordinates[0].name == "coordinate_1"
    assert description.coordinates[1].name == "coordinate_2"

    assert len(description.attributes) == 1
    assert description.attributes[0].name == "attribute_1"


@pytest.mark.skipif(not HAS_HDF, reason="pyhdf not available.")
def test_convert_to_xarray():
    """
    Converts test file to xarray dataset.
    """
    from pansat.formats.hdf4 import HDF4File
    description = ProductDescription(TEST_DATA)
    file_handle = HDF4File(TEST_FILE_HDF)
    dataset = description.to_xarray_dataset(file_handle)
