from pathlib import Path

import pytest

from pansat.products.satellite.meteosat import l1b_msg_seviri, l1b_msg_seviri_io

FILENAMES = {
    "MSG/MSG1-SEVI-MSG15-0100-NA-20210311101241.882000000Z-NA.nat": l1b_msg_seviri,
    "MSG4-SEVI-MSG15-0100-NA-20210311101243.952000000Z-NA.nat": l1b_msg_seviri_io,
}


@pytest.mark.parametrize("filename", FILENAMES)
def test_filename_regex(filename):
    """
    Ensure that file is properly recognized as MSG Seviri L1B file.
    """
    path = Path(filename)
    product = FILENAMES[filename]
    assert product.filename_regex.match(path.name)


@pytest.mark.parametrize("filename", FILENAMES)
def test_filename_to_date(filename):
    """
    Ensure that date extracted from filename is correct.
    """
    path = Path(filename)
    product = FILENAMES[filename]
    time = product.filename_to_date(path)

    assert time.year == 2021
    assert time.month == 3
    assert time.day == 11
    assert time.hour == 10
    assert time.minute == 12
