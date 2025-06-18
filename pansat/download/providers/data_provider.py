"""
pansat.download.providers.data_provider
=======================================

A data provider is an entity from which a specific data product can be downloaded.
A general data source, such as the Icare server or the Copernicus Climate Data
Store, are represented as classes of data providers. This module provides the
``DataProvider`` abstract base class (ABC) that defines the general interface for
data provider classes.
"""
from abc import ABC, abstractmethod, abstractclassmethod
from typing import Optional, List
from pathlib import Path

from pansat.geometry import Geometry
from pansat.time import TimeRange
from pansat.file_record import FileRecord
from pansat.products import Product


ALL_PROVIDERS = []


class DataProvider(ABC):
    """
    Abstract base class for data provider classes.
    """

    def __init__(self):
        ALL_PROVIDERS.append(self)

    @abstractmethod
    def provides(self, product: Product) -> bool:
        """
        Whether or not the given product is provided by this
        dataprovider.

        Args:
            product: A 'pansat.Product' object.

        Return:
            'True' if the product is available through this dataprovider.
            'False' otherwise.
        """
        return False

    @abstractmethod
    def download(
        self, file_record: FileRecord, destination: Optional[Path] = None
    ) -> FileRecord:
        """
        Download a product file to a given destination.

        Args:
            file_record: A FileRecord identifying the

        Return:
            An updated file record whose 'local_path' attribute points
            to the downloaded file.
        """

    @abstractmethod
    def find_files(
        self, product: Product, time_range: TimeRange, roi: Optional[Geometry]
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


class MetaDataprovider(ABC):
    """
    Abstract base class for data providers that also provide meta data.
    """

    @abstractclassmethod
    def get_available_products(cls):
        """
        Return the names of products available from this data provider.

        This method is used by each data product to determine whether it can
        use this provider to download data. The names provided here must therefore
        match the string representation returned by the product's ``__str__`` method.

        Return:
            A list of strings containing the names of the products that can
            be downloaded from this data provider.
        """

    @abstractmethod
    def get_metdata(self, file_record):
        """
        This method downloads data for a given time range from respective the
        data provider.

        Args:
            start(``datetime.datetime``): date and time for start
            end(``datetime.datetime``): date and time for end
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """


def get_providers() -> List["DataProvider"]:
    """
    Return a list of all currently known providers.
    """
    import pansat.download.providers.ges_disc
    import pansat.download.providers.iowa_state
    import pansat.download.providers.goes_aws
    import pansat.download.providers.himawari_aws
    import pansat.download.providers.nasa_nccs
    import pansat.download.providers.noaa_ncei
    import pansat.download.providers.eumetsat
    import pansat.download.providers.uci
    import pansat.download.providers.meteo_france
    import pansat.download.providers.cloudsat_dpc
    import pansat.download.providers.icare
    import pansat.download.providers.pmm_gv
    import pansat.download.providers.ecmwf
    import pansat.download.providers.nasa_pps
    import pansat.download.providers.ncar_stage4
    import pansat.environment as penv

    return ALL_PROVIDERS + [penv.get_active_registry()]
