"""
pansat.metadata
===============

This module defines the 'Metadata' data class, which holds metadata
for product files.
"""
from dataclasses import dataclass
from typing import Optional

from pansat.time import TimeRange
from pansat.geometry import Geometry


@dataclass
class Metadata:
    """
    The metadata for a product file contains the tempoeral coverage
    of the file and, optionally, a geometry object describing its
    spatial coverage.
    """

    time_range: TimeRange
    spatial_coverage: Optional[Geometry]
