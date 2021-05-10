"""
pansat.products.product
=======================

This module provides the Product abstract base class which defines the general
interface for objects representing data products.
"""
from abc import ABC, abstractproperty, abstractmethod


class Product(ABC):
    """
    The abstract interface for satellite and reanalysis data products.
    """

    @abstractproperty
    def default_destination(self):
        """Default folder structure used to store results."""

    @abstractmethod
    def filename_to_date(self, filename):
        """
        Extract data from filename.

        Args:
            filename(``str``): The filename.

        Returns:
            ``datetime`` object corresponding to the date encoded in the
            filename.
        """

    @abstractmethod
    def download(self, start_time, end_time, destination=None, provider=None):
        """
        Download data product for given time range.

        Args:
            start_time(``datetime``): ``datetime`` object defining the start date
                 of the time range.
            end_time(``datetime``): ``datetime`` object defining the end date of the
                 of the time range.
            destination(``str`` or ``pathlib.Path``): The destination where to store
                 the output data.
        """

    @abstractmethod
    def __str__(self):
        """Should return a string representation of the product name."""
