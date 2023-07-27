"""
pansat.products
===============

The ``products`` module provides functionality for handling supported data products.
"""
from abc import ABC, abstractmethod, abstractproperty
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import xarray as xr

from pansat.download.providers.copernicus import COPERNICUS_PRODUCTS
from pansat.download.providers.noaa import NOAA_PRODUCTS
from pansat.download.providers.icare import ICARE_PRODUCTS
from pansat.download.providers.ges_disc import GPM_PRODUCTS
from pansat.file_record import FileRecord
from pansat.time import TimeRange
from pansat import geometry

ALL_PRODUCTS = [
    *COPERNICUS_PRODUCTS,
    *NOAA_PRODUCTS,
    *list(ICARE_PRODUCTS.keys()),
    *list(GPM_PRODUCTS.keys()),
]


class Geometry:
    """A dummy class."""

    pass


class Product(ABC):
    """
    Generic interface for datasets managed by pansat.

    This class defines the essential functionality that a product must
    implement to be used inside the pansat framework.
    """
    PRODUCTS = {}

    def __init__(self):
        Product.PRODUCTS[self.name] = self

    @classmethod
    def get_product(cls, name):
        return cls.PRODUCTS[name]

    @abstractproperty
    def default_destination(self):
        """
        A relative directory to use to store files from this product
        to.
        """
        pass

    @abstractproperty
    def name(self):
        """
        A name that uniquely identifies a product.
        """
        pass

    @abstractmethod
    def matches(self, path: Path) -> bool:
        """
        Determine wheter a given file belongs to this product.

        Args:
            path: A path object pointing to a local file.

        Return:
            'True' if the file belongs to this product. 'False' otherwise.
        """

    @abstractmethod
    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Determine the temporal coverage of a data file.

        This function should try to deduce the time coverage of a data file
        from its filename.

        If it cannot be avoided, a local file may be opened to determine the
        time range. However, this should only be performed if the reading of
        the time range is efficient.

        Args:
            rec: A 'FileRecord' object representing the file from which to
                deduce the temporal coverage.

        Return:
            A 'TimeRange' object representing the time range covered by the
            data file.
        """

    @abstractmethod
    def get_spatial_coverage(self, rec: FileRecord) -> Geometry:
        """
        Determine the spatial coverage of a data file.

        Args:
            rec: A file record representing the file of which to determine
                the spatial extent.

        Return:
            A 'Geometry' object representing the spatial that the given
            datafile covers.
        """
        pass

    @abstractmethod
    def open(self, rec: FileRecord) -> xr.Dataset:
        """
        This function should read a given data file into an xarray.Dataset.

        Args:
            rec: A file record representing the file to be opened.

        Return:
            An 'xarray.Dataset' that contains the data of the provided file.
        """
        pass

    def download(
            self,
            start_time,
            end_time=None,
            destination=None
    ):
        if end_time is None:
            end_time = start_time

        if destination is None:
            destination = self.default_destination

        t_range = TimeRange(start_time, end_time)
        for provider in ALL_PROVIDERS:
            if provider.provides(self):
                provider.download(self, t_range, destination)



class NetcdfProduct(ABC):
    """
    A generic product interface for data in NetCDF format with certain standard
    names.
    """

    def __init__(self, variable_names=None):
        super().__init__()
        self.variable_names = variable_names

    def _get_variable_name(self, name):
        if self.variable_names is None:
            return name
        return self.variable_names.get(name, name)

    def matches(self, path: Path) -> bool:
        """
        Determine wheter a given file belongs to this product.

        Args:
            path: A path object pointing to a local file.

        Return:
            'True' if the file belongs to this product. 'False' otherwise.
        """
        return True

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Determine the temporal coverage of a data file.

        This function should try to deduce the time coverage of a data file
        from its filename.

        If it cannot be avoided, a local file may be opened to determine the
        time range. However, this should only be performed if the reading of
        the time range is efficient.

        Args:
            rec: A 'FileRecord' object representing the file from which to
                deduce the temporal coverage.

        Return:
            A 'TimeRange' object representing the time range covered by the
            data file.
        """
        if rec.local_path is not None and rec.local_path.exists():
            with xr.open_dataset(rec.local_path) as input_data:
                time_var = self._get_variable_name("time")
                start_time = input_data[time_var].data.min()
                end_time = input_data[time_var].data.max()
                return TimeRange(start_time, end_time)
        raise ValueError(
            "A NetcdfProduct needs a local file to determine temporal coverage"
            " but the 'local_path' attribute of the provided file record "
            "does not point to an existing file."
        )

    def get_spatial_coverage(self, rec: FileRecord) -> Geometry:
        """
        Determine the spatial coverage of a data file.

        Args:
            rec: A file record representing the file of which to determine
                the spatial extent.

        Return:
            A 'Geometry' object representing the spatial that the given
            datafile covers.
        """
        if rec.local_path is not None and rec.local_path.exists():
            with xr.open_dataset(rec.local_path) as input_data:
                lons = input_data[self._get_variable_name("longitude")].data
                lats = input_data[self._get_variable_name("latitude")].data
                return geometry.parse_swath(lons, lats)

        raise ValueError(
            "A NetcdfProduct needs a local file to determine spatial coverage"
            " but the 'local_path' attribute of the provided file record "
            "does not point to an existing file."
        )

    def open(self, rec: FileRecord) -> xr.Dataset:
        """
        This function should read a given data file into an xarray.Dataset.

        Args:
            rec: A file record representing the file to be opened.

        Return:
            An 'xarray.Dataset' that contains the data of the provided file.
        """
        return xr.open_dataset(rec.local_path)


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
        return (
            (self.file_record.filename == other.file_record.filename) and
            (self.time_range.start == other.time_range.start) and
            (self.time_range.end == other.time_range.end)
        )

    def __hash__(self):
        return hash((
            self.file_record.filename,
            self.time_range.start,
            self.time_range.end,
            self.primary_index_name,
            self.primary_index_range,
            self.secondary_index_name,
            self.secondary_index_range,
        ))


class GranuleProduct(Product):
    """
    A granuled product is a product whose datafiles lend themselves
    to the representation using granules.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def get_granules(self, file_record: FileRecord) -> list[Granule]:
        """
        Return a list of granules representing the temporal and spatial
        coverage of the data files identified by the given file record.

        Args:
            file_record: A file record pointing to a data file.

        Return:
            A list of granule objects representing the temporal and spatial
            coverage of the data file.
        """

    @abstractmethod
    def open_granule(
            self,
            granule: Granule
    ) -> xr.Dataset:
        """
        Load data from a granule.

        Args:
            granule: The data represents the granule.

        Return:
            An xarray.Dataset containing the loaded data.
        """
