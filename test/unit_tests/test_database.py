"""
Tests for the pansat.database module.
"""
from pansat import FileRecord
from pansat.products.example import (
    hdf5_product,
    hdf5_granule_product
)
from pansat.catalog.index import Index
from pansat.database import (
    get_table_names,
    IndexData
)


def test_get_table_names(hdf5_product_data, tmp_path):
    """
    Ensure that saving index data into an SQLLite database
    works.
    """
    files = (hdf5_product_data / "remote").glob("*")
    index = Index.index(hdf5_product, files)

    index.save(tmp_path)
    assert (tmp_path / f"{hdf5_product.name}.db").exists()

    table_names = get_table_names(tmp_path / f"{hdf5_product.name}.db")
    assert len(table_names) == 1
    assert table_names[0] == "example.hdf5_product"


def test_index_data(hdf5_product_data, tmp_path):
    """
    Ensure that managing index data using an IndexData object works.
    """
    files = list((hdf5_product_data / "remote").glob("*"))
    index = Index.index(hdf5_product, files)

    # In-memory database.
    index_data = IndexData(hdf5_granule_product)
    index_data.insert(index.data.load())
    assert len(index_data) == len(index)

    index_data.insert(index.data.load())
    assert len(index_data) == len(index)


    loaded = index_data.load()
    assert (loaded == index.data.load()).all().all()

    # On-disk database.
    index_data.persist(tmp_path)

    loaded_data = IndexData(hdf5_granule_product, tmp_path)
    loaded_2 = loaded_data.load()
    assert (loaded == loaded_2).all().all()


def test_time_range(hdf5_product_data, tmp_path):
    """
    Ensure that calculating the time range covered by the index works.
    """
    files = list((hdf5_product_data / "remote").glob("*"))
    index = Index.index(hdf5_product, files)
    index_range = index.time_range
    for path in files:
        rec = FileRecord(files[0])
        trange = hdf5_product.get_temporal_coverage(rec)
        assert (index_range.start <= trange.start) and (index_range.end >= trange.end)


def test_get_local_path(hdf5_product_data, tmp_path):
    """
    Ensure that:
        - finding the local path of an existing local file returns
          the path of this file.
        - finding the local path of a non-existing file returns None
    """
    files = list((hdf5_product_data / "remote").glob("*"))
    index = Index.index(hdf5_product, files)

    rec = FileRecord(files[0])
    local_path = index.data.get_local_path(rec)
    assert local_path == files[0]

    empty = IndexData(hdf5_product)
    local_path = empty.get_local_path(rec)
    assert local_path is None
