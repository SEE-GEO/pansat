from pathlib import Path
from pansat.products.product_description import ProductDescription

TEST_DATA = Path(__file__).parent / "data" / "test_description.ini"

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



