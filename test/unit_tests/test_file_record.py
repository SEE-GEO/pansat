import json

from pansat.file_record import FileRecord
from pansat.products.example import hdf5_product


def test_json_serialization():
    """
    Test serialization of file records.
    """
    rec = FileRecord(
        "/local_path/product.h5",
        product=hdf5_product
    )
    json_repr = rec.to_json()

    def object_hook(dct):
        if "FileRecord" in dct:
            return FileRecord.from_dict(dct["FileRecord"])
        return dct

    rec_loaded = json.loads(json_repr, object_hook=object_hook)

    assert rec_loaded.filename == "product.h5"
    assert rec_loaded.product == hdf5_product
