import pytest
import pansat.products.satellite.cloud_sat as cloud_sat
from datetime import datetime

test_names = {
    "1B-CPR": "2018143004115_64268_CS_1B-CPR_GRANULE_P_R05_E07_F00.hdf"
    }

test_times = {
    "1B-CPR": datetime(2018, 5, 23, 00, 41, 15)
}

products = [cloud_sat.l1b_cpr]

@pytest.mark.parametrize("product", products)
def test_cloud_sat_products(product):
    filename = test_names[product.name]
    time = product.filename_to_date(filename)

    assert product.matches(filename)
    assert time == test_times[product.name]
