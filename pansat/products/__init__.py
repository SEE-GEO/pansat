"""
pansat.products
===============

The ``products`` module provides functionality for handling supported data products.
"""
from abc import ABC, abstractmethod, abstractproperty
from dataclasses import dataclass
import importlib
from pathlib import Path
from typing import Optional, List, Union

import xarray as xr

from pansat.file_record import FileRecord
from pansat.granule import Granule
from pansat.time import TimeRange
from pansat import geometry


class Geometry:
    """A dummy class."""

    pass


def get_product(product_name):
    """
    Retrieve a product by its name.

    Args:
        product_name: A string representing the product name which must mach
            the 'name' attribute of the product object.

    Return:
        The object representing the product.

    Raises:
        ValueError if no product with the given product name could
        be found.
    """
    if product_name in Product.PRODUCTS:
        return Product.PRODUCTS[product_name]

    parts = product_name.split(".")
    if len(parts) > 1:
        try:
            module = ".".join(["pansat", "products"] + parts[:-1])
            module = importlib.import_module(module)
            product = getattr(module, parts[-1])
            return product
        except (ImportError, AttributeError):
            pass
    raise ValueError(f"Could not find a product with the name '{product_name}'.")


def all_products() -> List["Product"]:
    """
    Return a list of all currently known products.
    """
    from pansat.products.satellite import gpm
    from pansat.products.ground_based import mrms
    return list(Product.PRODUCTS.values())


class Product(ABC):
    """
    Generic interface for all data products managed by pansat.

    This class defines the essential functionality that a product must
    implement to be used inside the pansat framework.
    """
    PRODUCTS = {}

    def __init__(self):
        Product.PRODUCTS[self.name] = self

    def __eq__(self, other):
        """
        Since a product's name uniquely identifies it, two products are equal
        when their names are equal.
        """
        return self.name == other.name

    @abstractproperty
    def default_destination(self) -> Path:
        """
        Name of a relative directory to use to store files from this product
        to.
        """
        pass

    @abstractproperty
    def name(self) -> str:
        """
        A name that uniquely identifies a product.
        """
        pass

    @abstractmethod
    def matches(self, rec: FileRecord) -> bool:
        """
        Determine wheter a given file belongs to this product.

        Args:
            rec: A file record object pointing to a local file.

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
            time_range: TimeRange,
            roi: Optional[Geometry] = None,
            destination=None
    ) -> List[FileRecord]:
        """
        Find and download available files within a given time range and
        optional geographic region.

        Args:
            product: A 'pansat.Product' object representing the product to
                download.
            time_range: A 'pansat.time.TimeRange' object representing the time
                range within which to look for available files.
            roi: An optional region of interest (roi) restricting the search
                to a given geographical area.

        Return:
            A list of 'pansat.FileRecords' specifying the downloaded
            files.
        """
        files = self.find_files(time_range, roi=roi)
        downloaded = []
        for rec in files:
            downloaded.append(rec.download(destination=destination))
        return downloaded

    def get(
            self,
            time_range: TimeRange,
            roi: Optional[Geometry] = None,
            destination=None
    ) -> List[FileRecord]:
        """
        Find available files within a given time range and optional
        geographic region. If any of these files are not found locally,
        they will be downloaded.

        Args:
            time_range: A 'pansat.time.TimeRange' object representing the time
                range within which to look for available files.
            roi: An optional region of interest (roi) restricting the search
                to a given geographical area.

        Return:
            A list of 'pansat.FileRecords' specifying the downloaded
            files.
        """
        files = self.find_files(time_range, roi=roi)
        local = []
        for rec in files:
            local.append(rec.get(destination=destination))
        return local

    def find_provider(self) -> Optional['Provider']:
        """
        Find provider for this product.

        Return:
            A data provider object providing this product or None.
        """
        from pansat.download.providers.data_provider import get_providers
        product_provider = None
        for provider in get_providers():
            try:
                if hasattr(provider, "provides"):
                    if provider.provides(self):
                        product_provider = provider
            except Exception:
                pass
        return product_provider


    def find_files(
            self,
            time_range: TimeRange,
            roi: Optional[Geometry] = None
    ) -> List[FileRecord]:
        """
        Find available files within a given time range and optional geographic
        region.

        Args:
            product: A 'pansat.Product' object representing the product to
                download.
            time_range: A 'pansat.time.TimeRange' object representing the time
                range within which to look for available files.
            roi: An optional region of interest (roi) restricting the search
                to a given geographical area.

        Return:
            A list of 'pansat.FileRecords' specifying the available
            files.
        """
        from pansat.download.providers.data_provider import get_providers
        product_provider = self.find_provider()
        if product_provider is None:
            raise RuntimeError(f"Could not find a provider for the product '{self}'.")
        return product_provider.find_files(self, time_range, roi=roi)

    def __str__(self):
        return f"Product(name='{self.name}')"


class FilenameRegexpMixin:
    """
    Mixin class providing a implementation of the 'matches' member function
    based on a class attribute 'filename_regexp' containing a regular expression
    to match to filename.
    """
    def matches(self, rec: Union[FileRecord, Path, str]) -> bool:
        """
        Determines whether a given filename matches the pattern used for
        the product.

        Args:
            rec: File record, path, or simply filename of a given file.

        Return:
            True if the filename matches the product, False otherwise.
        """
        filename = rec
        if isinstance(rec, Path):
            filename = rec.name
        elif isinstance(rec, FileRecord):
            filename = rec.filename
        return self.filename_regexp.match(filename) is not None


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


class GranuleProduct(Product):
    """
    A granuled product is a product whose datafiles lend themselves
    to the representation using granules.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def get_granules(self, file_record: FileRecord) -> List[Granule]:
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
    def open_granule(self, granule: Granule) -> xr.Dataset:
        """
        Load data from a granule.

        Args:
            granule: The data represents the granule.

        Return:
            An xarray.Dataset containing the loaded data.
        """

def list_products(module):
    """
    List all product instances that are available in a given module.

    Args:
        module: A module object containing Product objects.

    Return:
        A list of object products found within the given module.
    """
    products = []
    for obj in module.__dict__.values():
        if isinstance(obj, Product):
            products.append(obj)
    return sorted(products, key=lambda prod: prod.name)
