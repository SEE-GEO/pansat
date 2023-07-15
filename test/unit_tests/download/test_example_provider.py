"""
Test for the pansat.download.providers.example module.
"""
from datetime import datetime

import pytest
import conftest

from pansat.time import TimeRange
from pansat.products.example import hdf4_product, hdf5_product
from pansat.download.providers.example import ExampleProvider


@pytest.mark.parametrize("product_data", conftest.PRODUCT_DATA)
def test_example_provider(tmp_path, product_data, request):

    product_data = request.getfixturevalue(product_data)

    hdf4_files = list(product_data.glob("remote/*.hdf"))
    hdf5_files = list(product_data.glob("remote/*.h5"))

    if len(hdf4_files) > 0:
        format = "hdf4"
        product = hdf4_product
    else:
        format = "hdf5"
        product = hdf5_product

    provider = ExampleProvider(product_data, format)

    assert provider.provides(product)

    t_range = TimeRange(
        datetime(2020, 1, 1),
        datetime(2020, 1, 2)
    )
    files = provider.find_files(product, t_range)
    assert len(files) == 4

    t_range = TimeRange(
        datetime(2020, 1, 1),
        datetime(2020, 1, 1, 2)
    )
    files = provider.find_files(product, t_range)
    assert len(files) == 4

    local_path = tmp_path / "local"
    local_path.mkdir()

    for rec in files:
        rec.download(local_path)

    assert len(list(local_path.glob("*"))) == 4
