"""
Tests for the available PERSIANN products.
"""
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

import pansat.products.satellite.persiann
from pansat.products import get_product


FILENAMES = {
    "satellite.persiann.cdr_daily": (
        "aB1_d00001.bin.gz",
        datetime(2000, 1, 1)
    ),
    "satellite.persiann.cdr_monthly": (
        "aB1_m0001.bin.gz",
        datetime(2000, 1, 1)
    ),
    "satellite.persiann.cdr_yearly": (
        "aB1_m00.bin.gz",
        datetime(2000, 1, 1)
    ),
    "satellite.persiann.ccs_3h": (
        "rgccs3h0300100.bin.gz",
        datetime(2003, 1, 1)
    ),
    "satellite.persiann.ccs_6h": (
        "rgccs3h0300100.bin.gz",
        datetime(2003, 1, 1)
    ),
    "satellite.persiann.ccs_daily": (
        "rgccs3h03001.bin.gz",
        datetime(2003, 1, 1)
    ),
    "satellite.persiann.ccs_monthly": (
        "rgccs3h0301.bin.gz",
        datetime(2003, 1, 1)
    ),
    "satellite.persiann.ccs_yearly": (
        "rgccs3h03.bin.gz",
        datetime(2003, 1, 1)
    ),
}

def test_persiann_products():
    """
    Ensure that defined PERSIANN products match filenames and start times.
    """
    for product_name, (filename, start_time) in FILENAMES.items():
        prod = get_product(product_name)
        assert prod.matches(filename)

        temporal_coverage = prod.get_temporal_coverage(filename)
        assert temporal_coverage.start == start_time
