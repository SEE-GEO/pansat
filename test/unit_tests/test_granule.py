import pytest

import numpy as np

from pansat.geometry import LonLatRect
from pansat.granule import Granule, merge_granules
from pansat.file_record import FileRecord
from pansat.time import TimeRange


@pytest.fixture()
def test_granules():
    """
    Creates 6 granules of which the first ones are contiguous granules
    in one file and the other two granules are two contiguous granules
    in a second file.

    Return:

        A list containing the 6 granules.

    """
    granule_1 = Granule(
        FileRecord("file_1"),
        TimeRange(
            np.datetime64("2020-01-01T00:00:00"),
            np.datetime64("2020-01-01T00:05:00"),
        ),
        LonLatRect(0, 0, 5, 5),
        "scans",
        (0, 100),
        "pixels",
        (0, 100)
    )

    granule_2 = Granule(
        FileRecord("file_1"),
        TimeRange(
            np.datetime64("2020-01-01T00:00:00"),
            np.datetime64("2020-01-01T00:05:00"),
        ),
        LonLatRect(-5, -5, 0, 0),
        "scans",
        (0, 100),
        "pixels",
        (100, 200)
    )

    granule_3 = Granule(
        FileRecord("file_1"),
        TimeRange(
            np.datetime64("2020-01-01T00:05:00"),
            np.datetime64("2020-01-01T00:10:00"),
        ),
        LonLatRect(5, 0, 10, 5),
        "scans",
        (100, 200),
        "pixels",
        (0, 100)
    )

    granule_4 = Granule(
        FileRecord("file_1"),
        TimeRange(
            np.datetime64("2020-01-01T00:05:00"),
            np.datetime64("2020-01-01T00:10:00"),
        ),
        LonLatRect(5, -5, 10, 0),
        "scans",
        (100, 200),
        "pixels",
        (100, 200)
    )

    granule_5 = Granule(
        FileRecord("file_2"),
        TimeRange(
            np.datetime64("2020-01-01T00:10:00"),
            np.datetime64("2020-01-01T00:15:00"),
        ),
        LonLatRect(10, 0, 15, 5),
        "scans",
        (0, 100),
        "pixels",
        (0, 100)
    )

    granule_6 = Granule(
        FileRecord("file_2"),
        TimeRange(
            np.datetime64("2020-01-01T00:10:00"),
            np.datetime64("2020-01-01T00:15:00"),
        ),
        LonLatRect(10, -5, 15, 0),
        "scans",
        (0, 100),
        "pixels",
        (100, 200)
    )

    return [
        granule_1,
        granule_2,
        granule_3,
        granule_4,
        granule_5,
        granule_6
    ]


def test_equality(test_granules):
    for ind_1, granule_1 in enumerate(test_granules):
        for ind_2, granule_2 in enumerate(test_granules):
            if ind_1 == ind_2:
                assert granule_1 == granule_2
            else:
                assert not granule_1 == granule_2


def test_hash(test_granules):
    granules = set(test_granules)
    assert len(granules) == 6


def test_adjacent(test_granules):

    g_1, g_2, g_3, g_4, g_5, g_6 = test_granules

    assert g_1.is_adjacent(g_1)
    assert g_1.is_adjacent(g_2)
    assert g_1.is_adjacent(g_3)
    assert g_1.is_adjacent(g_4)
    assert not g_1.is_adjacent(g_5)
    assert not g_1.is_adjacent(g_6)

    assert g_2.is_adjacent(g_1)
    assert g_2.is_adjacent(g_2)
    assert g_2.is_adjacent(g_3)
    assert g_2.is_adjacent(g_4)
    assert not g_2.is_adjacent(g_5)
    assert not g_2.is_adjacent(g_6)

    assert g_3.is_adjacent(g_1)
    assert g_3.is_adjacent(g_2)
    assert g_3.is_adjacent(g_3)
    assert g_3.is_adjacent(g_4)
    assert not g_3.is_adjacent(g_5)
    assert not g_3.is_adjacent(g_6)

    assert g_4.is_adjacent(g_1)
    assert g_4.is_adjacent(g_2)
    assert g_4.is_adjacent(g_3)
    assert g_4.is_adjacent(g_4)
    assert not g_4.is_adjacent(g_5)
    assert not g_4.is_adjacent(g_6)


def test_merge(test_granules):

    g_1, g_2, g_3, g_4, g_5, g_6 = test_granules
    g_m_1 = g_1.merge(g_2.merge(g_3.merge(g_4)))

    g_m_2 = g_5.merge(g_6)

    assert not g_m_1.is_adjacent(g_6)


def test_merge_granules(test_granules):

    granules = merge_granules(test_granules)
    assert len(granules) == 2
