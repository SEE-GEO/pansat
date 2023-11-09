import json

from pansat.file_record import FileRecord
from pansat.products.example import hdf5_product
from pansat.time import TimeRange


def test_json_serialization():
    """
    Test serialization of file records.
    """
    rec = FileRecord("/local_path/product.h5", product=hdf5_product)
    json_repr = rec.to_json()

    def object_hook(dct):
        if "FileRecord" in dct:
            return FileRecord.from_dict(dct["FileRecord"])
        return dct

    rec_loaded = json.loads(json_repr, object_hook=object_hook)

    assert rec_loaded.filename == "product.h5"
    assert rec_loaded.product == hdf5_product


def test_find_closest(hdf5_product_provider):
    """
    Ensure that search for temporally close file records returns
    the right files.
    """
    time_range = TimeRange(
        "2020-01-01T00:00:00",
        "2020-01-02T00:00:00"
    )
    records = hdf5_product.get(time_range)

    rec = records[0]
    other = rec.find_closest_in_time(records)
    assert len(other) == 2
    assert other[0] is rec

    other = rec.find_closest_in_time(records[2:])
    assert len(other) == 1
    assert other[0] is records[2]
