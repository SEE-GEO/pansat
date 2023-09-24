"""
pansat.granule
==============

This module defines the granule class, which is used to represent
the spatial and temporal extent of contiguous pieces of geospatial
data.
"""
from dataclasses import dataclass
import json
from typing import Optional

from pansat.file_record import FileRecord
from pansat.time import TimeRange
from pansat.geometry import Geometry, ShapelyGeometry


@dataclass
class Granule:
    """
    Granules represent temporally and spatially limited sub-sections of a
    data file. Their purpose is to allow for more find-grained data
    retrieval.
    """

    file_record: FileRecord
    time_range: TimeRange
    geometry: Geometry
    primary_index_name: str = ""
    primary_index_range: tuple[int] = (-1, -1)
    secondary_index_name: str = ""
    secondary_index_range: Optional[tuple[int]] = (-1, -1)

    def get_slices(self):
        """
        Get slice dictionary identifying the data range corresponding to
        this granule.
        """
        if self.primary_index_name == "":
            return None
        slcs = {}
        slcs[self.primary_index_name] = slice(*self.primary_index_range)
        if self.secondary_index_name != "":
            slcs[self.secondary_index_name] = slice(*self.secondary_index_range)
        return slcs

    def __eq__(self, other):
        """
        To be considered equal, two granules must point to the same file,
        and the same primary and seconday index range.
        """
        return (
            (self.file_record.filename == other.file_record.filename)
            and (self.primary_index_name == other.primary_index_name)
            and (self.primary_index_range == other.primary_index_range)
            and (self.secondary_index_name == other.secondary_index_name)
            and (self.secondary_index_range == other.secondary_index_range)
        )

    def __hash__(self):
        """
        The hash of a granule is computed using its filename and the
        names and ranges of the primary and secondary indices.
        """
        return hash(
            (
                self.file_record.filename,
                self.primary_index_name,
                self.primary_index_range,
                self.secondary_index_name,
                self.secondary_index_range,
            )
        )

    def __repr__(self):
        if self.secondary_index_name == "":
            return (
                f"Granule(filename='{self.file_record.filename}', "
                f"start_time='{self.time_range.start}', "
                f"end_time='{self.time_range.end}', "
                f"primary_index_name='{self.primary_index_name}', "
                f"primary_index_range='{self.primary_index_range}')"
            )
        return (
            f"Granule(filename='{self.file_record.filename}', "
            f"start_time='{self.time_range.start}', "
            f"end_time='{self.time_range.end}', "
            f"primary_index_name='{self.primary_index_name}', "
            f"primary_index_range='{self.primary_index_range}', "
            f"secondary_index_name='{self.secondary_index_name}', "
            f"secondary_index_range='{self.secondary_index_range}')"
        )

    def is_adjacent(self, other):
        """
        Determine whether two granules are adjacent or overlapping.

        Two granules are considered adjacent if the point to the same file
        and when either their primary indices or secondary indices are
        contiguous.

        Args:
            other: Another granule

        Return:
            'True' if the granules are adjacent.
        """
        if self.file_record.filename != other.file_record.filename:
            return False

        if self.primary_index_name != other.primary_index_name:
            return False

        if self.secondary_index_name != other.secondary_index_name:
            return False

        if self.primary_index_range[0] > other.primary_index_range[1]:
            return False

        if self.primary_index_range[1] < other.primary_index_range[0]:
            return False

        if self.secondary_index_range[0] > other.secondary_index_range[1]:
            return False

        if self.secondary_index_range[1] < other.secondary_index_range[0]:
            return False

        return True

    def merge(self, other):
        """
        Merge two granules.

        Merging two granules yields a new granule which covers the
        union of the spatial and temporal extent of the two original
        granules. Two granules must be adjacent to be merged.

        NOTE: Merging two granules may lead the granules geometry to
            underestimate the spatial extent of the data.

        Return:
             A new granule representing the union of the granules 'self'
             and other.
        """
        if not self.is_adjacent(other):
            raise ValueError("Can only merge adjacent granules.")

        time_range = TimeRange(
            min(self.time_range.start, other.time_range.end),
            max(self.time_range.start, other.time_range.end),
        )
        geometry = self.geometry.merge(other.geometry)

        primary_index_range = (
            min(self.primary_index_range[0], other.primary_index_range[0]),
            max(self.primary_index_range[1], other.primary_index_range[1]),
        )

        secondary_index_range = (
            min(self.secondary_index_range[0], other.secondary_index_range[0]),
            max(self.secondary_index_range[1], other.secondary_index_range[1]),
        )

        return Granule(
            self.file_record,
            time_range,
            geometry,
            self.primary_index_name,
            primary_index_range,
            self.secondary_index_name,
            secondary_index_range,
        )

    @classmethod
    def from_dict(cls, dct):
        """
        Load granule from dictionary representation.

        Args:
            dct: A dictionary representing a Granule object.

        Return:
            The deserialized granule object.
        """
        file_record = FileRecord.from_dict(dct.pop("file_record"))
        time_range = TimeRange.from_dict(dct.pop("time_range"))
        geometry = ShapelyGeometry.from_geojson(dct.pop("geometry"))
        primary_index_range = tuple(dct.pop("primary_index_range"))
        secondary_index_range = tuple(dct.pop("secondary_index_range"))
        return Granule(
            file_record=file_record,
            time_range=time_range,
            geometry=geometry,
            primary_index_range=primary_index_range,
            secondary_index_range=secondary_index_range,
            **dct,
        )

    def open(self):
        product = self.file_record.product
        return product.open(self.file_record.local_path, slcs=self.get_slices())

    def to_dict(self):
        """
        Return dictionary representation of this granule using only primitive
        (i.e., JSON encodable) types.
        """
        return {
            "file_record": self.file_record.to_dict(),
            "time_range": self.time_range.to_dict(),
            "geometry": self.geometry.to_geojson(),
            "primary_index_name": self.primary_index_name,
            "primary_index_range": self.primary_index_range,
            "secondary_index_name": self.secondary_index_name,
            "secondary_index_range": self.secondary_index_range,
        }

    def to_json(self):
        """
        Return json representation of this Granule object.
        """
        return json.dumps({"Granule": self.to_dict()})


class GranuleEncoder(json.JSONEncoder):
    """
    Encoder class that allows encoding granules in

    """

    def default(self, obj):
        if isinstance(obj, Granule):
            return {"Granule": obj.to_dict()}
        return json.JSONEncoder.default(self, obj)


def granule_hook(dct):
    """
    Object hook that allows loading Granule object from a json stream.
    """
    if "Granule" in dct:
        return Granule.from_dict(dct["Granule"])
    return dct


def merge_granules(granules):
    """
    Consecutively merges granules in list.

    NOTE: This function assumes the given granules to be ordered according
    to their start time so that adjacent granules are at consecutive locations
    in the list.

    Args:
        granules: List of granules to merge.

    Return:

        A new list of granules in which all adjacent granules were merged.
    """
    merged = []

    if len(granules) == 0:
        raise ValueError("Need at least one granule to merge.")

    current = granules[0]
    for ind in range(len(granules) - 1):
        try:
            current = current.merge(granules[ind + 1])
        except ValueError:
            merged.append(current)
            current = granules[ind + 1]

    merged.append(current)

    return merged
