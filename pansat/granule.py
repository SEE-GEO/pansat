"""
pansat.granule
==============

This module defines the granule class, which is used to represent
the spatial and temporal extent of contiguous pieces of geospatial
data.
"""
from dataclasses import dataclass
import json
from typing import Optional, Tuple

from pansat.file_record import FileRecord
from pansat.time import TimeRange
from pansat.geometry import Geometry, ShapelyGeometry
from pansat.products.product_description import get_slice, _geometry_from_coords


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
    primary_index_range: Tuple[int] = (-1, -1)
    secondary_index_name: str = ""
    secondary_index_range: Optional[Tuple[int]] = (-1, -1)

    @staticmethod
    def from_file_record(rec: FileRecord) -> ["Granule"]:
        """
        Create granule from file record.

        Args:
            rec: FileRecord pointing to a data file to represent as
                a granule.

        Return:
            A list of granules representing the temporal and spatial
            coverage of the given file.
        """
        from pansat.products import GranuleProduct
        prod = rec.product

        if isinstance(prod, GranuleProduct):
            return prod.get_granules(rec)

        time_range = prod.get_temporal_coverage(rec)
        geometry = prod.get_spatial_coverage(rec)

        return [
            Granule(rec, time_range, geometry)
        ]


    def get_slices(self):
        """
        Get slice dictionary identifying the data range corresponding to
        this granule.
        """
        if self.primary_index_name == "":
            return None
        slcs = {}
        slcs[self.primary_index_name] = slice([int(ind) for ind in self.primary_index_range])
        if self.secondary_index_name != "":
            slcs[self.secondary_index_name] = slice([int(ind) for ind in self.secondary_index_range])
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

    def __lt__(self, other: "Granule") -> bool:
        return self.time_range.start < other.time_range.start

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
        return []

    current = granules[0]
    for ind in range(len(granules) - 1):
        try:
            current = current.merge(granules[ind + 1])
        except ValueError:
            merged.append(current)
            current = granules[ind + 1]

    merged.append(current)

    return merged


def get_granules_from_dataset(
        dataset,
        time_coordinate = "time",
        longitude_coordinate = "longitude",
        latitude_coordinate = "latitude",
        partitions = (16, 1),
        resolution = (4, 4)
):
    """
    Extract granule representation from an xarray Dataset.

    Args:
        dataset: The xarray.Dataset from which to extract the granule
            reprsentation.
        file_handle: A file handle object providing access to a product
            data file.
        context: A Python context holding potential callback functions
            required for the loading of data.

    Return:
        A list of tuples ``(t_rng, geom, prm_ind_name, prm_ind_rng)``
        containing the time range, geometry and primary index name and
        range of each granule in the file.

        If the granuling takes place over two dimension each tuple
        additionally contains the name and range of the secondary
        index.
    """
    if time_coordinate not in dataset:
        raise ValueError(
            f"The time coordinate '{time_coordinate}' is not present "
            "in the dataset."
        )
    if latitude_coordinate not in dataset:
        raise ValueError(
            f"The latitude coordinate '{latitude_coordinate}' is not present "
            "in the dataset."
        )
    if longitude_coordinate not in dataset:
        raise ValueError(
            f"The longitude coordinate '{longitude_coordinate}' is not present "
            "in the dataset."
        )


    lons = dataset[longitude_coordinate].data
    lats = dataset[latitude_coordinate].data
    time = dataset[time_coordinate].data
    if time.ndim > lons.ndim:
        sizes = time.shape
    else:
        sizes = lons.shape

    dim_names = dataset[longitude_coordinate].dims[:2]

    granule_data = []

    outer_start = 0
    while outer_start < sizes[0] - 1:
        outer_end = min(
            outer_start + max(sizes[0] // partitions[0], 2),
            sizes[0],
        )

        outer_slc = get_slice(
            outer_start, outer_end, sizes[0], resolution[0]
        )

        if outer_start == outer_end:
            break
        outer_start = outer_end

        if len(sizes) == 1:
            inner_stop = 1
        else:
            inner_stop = sizes[1]
        inner_start = 0

        while inner_start < inner_stop:
            if len(sizes) == 1:
                slcs = {
                    dim_names[0]: outer_slc,
                }
                inner_start = inner_stop
            else:
                if len(partitions) == 1:
                    inner_end = sizes[1]
                else:
                    inner_end = inner_start + max(
                        sizes[1] // partitions[1], 2
                    )
                inner_slc = get_slice(
                    inner_start,
                    inner_end,
                    inner_stop,
                    resolution[1],
                )
                slcs = {
                    dim_names[0]: outer_slc,
                    dim_names[1]: inner_slc,
                }
                if inner_start == inner_end:
                    break
                inner_start = inner_end

            start_time = time.min()
            end_time = time.max()
            time_range = TimeRange(start_time, end_time)

            geom = _geometry_from_coords(lons, lats)

            if len(dim_names) == 1:
                granule_data.append(
                    (
                        time_range,
                        geom,
                        dim_names[0],
                        (outer_slc.start, outer_slc.stop),
                    )
                )
            else:
                granule_data.append(
                    (
                        time_range,
                        geom,
                        dim_names[0],
                        (outer_slc.start, outer_slc.stop),
                        dim_names[1],
                        (inner_slc.start, inner_slc.stop),
                    )
                )

    return granule_data
