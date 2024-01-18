"""
Tests for the pansat.database module.
"""
from pansat.products.example import (
    hdf5_granule_product
)
from pansat.catalog.index import Index
from pansat.database import (
    save_index_data,
    load_index_data,
    get_table_names
)


def test_index_serialization(hdf5_product_data, tmp_path):
    """
    Ensure that saving index data into an SQLLite database
    works.
    """
    files = (hdf5_product_data / "remote").glob("*")
    index = Index.index(hdf5_granule_product, files)

    save_index_data(index.product, index.data, tmp_path / "pansat.db")
    assert (tmp_path / "pansat.db").exists()

    data_loaded = load_index_data(
        hdf5_granule_product,
        tmp_path / "pansat.db"
    )
    data = index.data.sort_values("start_time").reset_index(drop=True)
    assert data.shape == data_loaded.shape
    assert (data == data_loaded).all().all()


def test_get_table_names(hdf5_product_data, tmp_path):
    """
    Ensure that saving index data into an SQLLite database
    works.
    """
    files = (hdf5_product_data / "remote").glob("*")
    index = Index.index(hdf5_granule_product, files)

    save_index_data(index.product, index.data, tmp_path / "pansat.db")
    assert (tmp_path / "pansat.db").exists()

    table_names = get_table_names(tmp_path / "pansat.db")
    assert len(table_names) == 1
    assert table_names[0] == "example.hdf5_granule_product"
