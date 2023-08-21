"""
pansat.products.satellite.persiann
==================================

This module provides and interface to download and open data from the
PERSIANN suite or precipitation retrieval algorithms.
"""
from datetime import datetime, timedelta
import gzip
from pathlib import Path
import re

import numpy as np
import xarray as xr

import pansat.download.providers as providers
from pansat.products.product import Product


class PersiannProduct(Product):
    """
    Base class for PERSIANN precipitation products.
    """

    def __init__(self, resolution=1):
        self.resolution = resolution

    def filename_to_date(self, filename):
        """
        Determine the data from a given filename.

        Args:
            filename: The name of the file of which to determine the data.

        Return:
            A 'datetime.datetime' object representing the date corresponding
            to the given file.
        """
        filename = str(filename)
        year = 2000 + int(filename[-14:-12])
        day_of_year = int(filename[-12:-9])
        hour = int(filename[-9:-7])
        date = datetime(year=year, month=1, day=1, hour=hour) + timedelta(
            days=day_of_year - 1
        )
        return date

    def open(self, filename):
        """
        Open file as 'xarray.Dataset'.

        Args:
            filename: Path to the file to open.

        Return:
            An 'xarray.Dataset' containing the data from the given
            file.
        """
        bytes = gzip.open(filename).read()
        shape = (3000, 9000)

        data = np.frombuffer(bytes, ">i2").reshape(shape)
        lons = np.linspace(0.02, 359.98, 9000)
        lats = np.linspace(59.98, -59.98, 3000)

        date = self.filename_to_date(filename)

        data = data / 100
        data[data < 0] = np.nan

        dataset = xr.Dataset(
            {
                "time": (("time",), [date]),
                "latitude": (("latitude",), lats),
                "longitude": (("longitude",), lons),
                "precipitation": (("time", "latitude", "longitude"), data[np.newaxis]),
            }
        )
        return dataset

    def _get_provider(self):
        """Find a provider that provides the product."""
        available_providers = [
            p
            for p in providers.ALL_PROVIDERS
            if str(self) in p.get_available_products()
        ]
        if not available_providers:
            raise NoAvailableProvider(
                f"Could not find a provider for the" f" product {str(self)}."
            )
        return available_providers[0]

    def download(self, start_time, end_time, destination=None, provider=None):
        """
        Download data product for given time range.

        Args:
            start_time(``datetime``): ``datetime`` object defining the start
                 date of the time range.
            end_time(``datetime``): ``datetime`` object defining the end date
                 of the of the time range.
            destination(``str`` or ``pathlib.Path``): The destination where to
                 store the output data.
        """

        if not provider:
            provider = self._get_provider()

        if not destination:
            destination = self.default_destination
        else:
            destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        provider = provider(self)

        return provider.download(start_time, end_time, destination)


class CCS(PersiannProduct):
    """
    The PERSIANN cloud classification system (CCS) precipitation retrieval.
    """

    RESOLUTIONS = {1: "hrly", 3: "3hrly", 6: "6hrly", 24: "daily"}

    def __init__(self, resolution=1):
        self.filename_regexp = re.compile("rgccs1h(\d{7}).bin.gz")
        super().__init__(resolution=resolution)

    @property
    def default_destination(self):
        """
        Files are stored in a 'PERSIANN-CCS' sub-folder.
        """
        return Path("PERSIANN-CCS")

    def __str__(self):
        return "PERSIANN-CCS"

    def get_path(self, year):
        """
        Use by the provider to determine the folder in which the
        files corresponding this product are found.
        """
        path = f"PERSIANN-CCS/{CCS.RESOLUTIONS[self.resolution]}"
        if self.resolution == 1:
            path = path + f"/{year}"
        return path


class PDIRNow(PersiannProduct):
    """
    The PERSIANN cloud classification system (CCS) precipitation retrieval.
    """

    RESOLUTIONS = {1: "1hourly", 3: "3hourly", 6: "6hourly"}

    def __init__(self, resolution=1):
        super().__init__(resolution=resolution)

    @property
    def default_destination(self):
        """
        Files are stored in a 'PERSIANN-CCS' sub-folder.
        """
        return Path("PDIRNow")

    def filename_to_date(self, filename):
        """
        Determine the data from a given filename.

        Args:
            filename: The name of the file of which to determine the data.

        Return:
            A 'datetime.datetime' object representing the date corresponding
            to the given file.
        """
        filename = str(filename)
        year = 2000 + int(filename[-15:-13])
        month = int(filename[-13:-11])
        day = int(filename[-11:-9])
        hour = int(filename[-9:-7])
        date = datetime(year=year, month=month, day=day, hour=hour)
        return date

    def __str__(self):
        return "PDIRNow"

    def get_path(self, year):
        """
        Use by the provider to determine the folder in which the
        files corresponding this product are found.
        """
        path = f"PDIRNow/PDIRNow{PDIRNow.RESOLUTIONS[self.resolution]}"
        if self.resolution == 1:
            path = path + f"/{year}"
        return path
