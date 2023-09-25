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

ALL_PROVIDERS = []


class DataProvider(ABC):
    """
    Abstract base class for data provider classes.
    """

    def __init__(self):
        ALL_PROVIDERS.append(self)

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
    def provides(self, product):
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
    def download(self, start, end, destination=None):
        """
        This method downloads data for a given time range from respective the
        data provider.

        Args:
            start(``datetime.datetime``): date and time for start
            end(``datetime.datetime``): date and time for end
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """


class MetaDataprovider(ABC):
    """
    Abstract base class for metadata provider.
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
