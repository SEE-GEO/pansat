"""
pansat.download.providers.ges_disc
==================================

This module provides a data provider for NASA's Goddard Earth Sciences Data and
Information Services Center (`GES DISC <https://disc.gsfc.nasa.gov/>`_).

Reference
---------
"""
import datetime
import json
import logging
import os
import pathlib
import re
import tempfile
import xml.etree.ElementTree as ET

import requests

from pansat.download import accounts
from pansat.download.providers.discrete_provider import (
    DiscreteProviderDay,
    DiscreteProviderMonth,
    DiscreteProviderYear
)
from pansat.file_record import FileRecord
from pansat.time import to_datetime

_DATA_FOLDER = pathlib.Path(__file__).parent / "data"
with open(_DATA_FOLDER / "gpm_products.json", "r") as file:
    GPM_PRODUCTS = json.load(file)


LOGGER = logging.getLogger(__file__)

class GesDiscProviderBase():
    """
    Dataprovider class for for products available from the
    gpm1.gesdisc.eosdis.nasa.gov domain.
    """
    file_pattern = re.compile('"[^"]*\.(?:HDF5|h5|nc|nc4)"')

    @classmethod
    def get_available_products(cls):
        """
        Return the names of products available from this data provider.

        Return:
            A list of strings containing the names of the products that can
            be downloaded from this data provider.
        """
        return GPM_PRODUCTS.keys()


    def download_url(self, url, destination):
        """
        Downloads file from GES DISC server using the 'GES DISC' identity
        and taking care of the URL redirection.
        """
        auth = accounts.get_identity("GES DISC")
        # If only requests.get(url, auth=auth) is used, the requests library
        # will search in ~/.netrc credentials for the machine
        # urs.earthdata.nasa.gov, but `auth` is not used as the authorization
        # is after a redirection
        # The method below handles the authorization after a redirection
        with requests.Session() as session:
            # Set credentials
            session.auth = auth

            # Get data
            redirect = session.get(url)
            response = session.get(redirect.url, auth=auth, stream=True)
            response.raise_for_status()

            # Write to disk
            with open(destination, "wb") as f:
                for chunk in response:
                    f.write(chunk)


    def get_base_url(self, product):
        """
        URL pointing to the root of the directory tree containing the
        data files of the given product.

        Args:
            product: The product for which to retrieve the URL.

        Return:
            The URL as a string.
        """
        base_url, path = GPM_PRODUCTS[product.name]
        return "/".join([base_url, path])

    def _request_string(self, product):
        """The URL containing the data files for the given product."""
        base_url = self.get_base_url(product)
        return base_url + "/{year}/{day}/{filename}"

    def find_files_by_year(self, product, year):
        """
        Return list of available files for a given day of a year.

        Args:
            year(``int``): The year for which to look up the files.

        Return:
            A list of strings containing the filenames that are available
            for the given year.
        """
        request_string = self._request_string(product).format(year=year, day="", filename="")
        auth = accounts.get_identity("GES DISC")
        response = requests.get(request_string, auth=auth)
        files = list(set(GesDiscProvider.file_pattern.findall(response.text)))
        return [f[1:-1] for f in files]

    def download_file(self, product, filename, destination):
        """
        Download file from data provider.

        Args:
            filename(``str``): The name of the file to download.
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """
        time = product.filename_to_date(filename)
        year = time.year
        day = time.strftime("%j")
        day = "0" * (3 - len(day)) + day
        if product.variant in ["MO"]:
            day = ""
        if product.variant.startswith("DAY"):
            day = f"{time.month:02}"
        url = self._request_string(product).format(year=year, day=day, filename=filename)
        self.download_url(url, destination)

    def download_metadata(self, filename):
        """
        Download metadata for given file.

        Args:
            filename(``str``): The name of the file to download.

        Return:
            The metadata in XML formaton
        """
        t = self.product.filename_to_date(filename)
        year = t.year
        day = t.strftime("%j")
        day = "0" * (3 - len(day)) + day
        url = (
            self._request_string.format(year=year, day=day, filename=filename) + ".xml"
        )

        response = requests.get(url)
        return ET.fromstring(response.text)


class GesDiscProviderDay(GesDiscProviderBase, DiscreteProviderDay):

    def provides(self, product):
        name = product.name
        if not name.startswith("satellite.gpm"):
            return False
        if hasattr(product, "variant"):
            if product.variant.startswith("mo"):
                return False
            if product.variant.startswith("day"):
                return False
        return True


    def find_files_by_day(self, product, time, roi=None):
        """
        Find files available data files at a given day.

        Args:
            product: A 'pansat.Product' object identifying the product
               for which to retrieve available data files.
            time: A time object specifying the day for which to retrieve
               available products.
            roi: An optional geometry object to limit the files to
               only those that cover a certain geographical region.

        Return:
            A list of file records identifying the files from the requested
            day.
        """
        time = to_datetime(time)
        rel_url = time.strftime("/%Y/%j")
        url = self.get_base_url(product) + rel_url
        auth = accounts.get_identity("GES DISC")
        response = requests.get(url, auth=auth)
        response.raise_for_status()

        files = list(set(self.file_pattern.findall(response.text)))
        files = [f[1:-1] for f in files]
        recs = [
            FileRecord.from_remote(product, self, url + f"/{fname}", fname)
            for fname in files
        ]
        return recs


class GesDiscProviderMonth(GesDiscProviderBase, DiscreteProviderMonth):
    def provides(self, product):
        name = product.name
        if not name.startswith("satellite.gpm"):
            return False
        if hasattr(product, "variant"):
            if product.variant.startswith("day"):
                return True
        return False

    def find_files_by_month(self, product, time) -> list[FileRecord]:
        """
        Return list of available files for a given day of a year.

        Args:


        Return:
            A list of strings containing the filenames that are available
            for the given year.
        """
        url = self.get_base_url(product) + f"/{time.year}/{time.month:02}"
        auth = accounts.get_identity("GES DISC")
        response = requests.get(url, auth=auth)
        response.raise_for_status()
        files = list(set(self.file_pattern.findall(response.text)))

        files = [f[1:-1] for f in files]
        recs = [
            FileRecord.from_remote(product, self, url + f"/{fname}", fname)
            for fname in files
        ]
        return recs

class GesDiscProviderYear(GesDiscProviderBase, DiscreteProviderYear):
    def provides(self, product):
        name = product.name
        if not name.startswith("satellite.gpm"):
            return False
        if hasattr(product, "variant"):
            if product.variant.startswith("mo"):
                return True
        return False

    def find_files_by_year(
            self,
            product,
            time,
            roi=None
    ) -> list[FileRecord]:
        """
        Return list of available files for a given day of a year.

        Args:
            product: A 'pansat.Product' object identifying the product
                whose data files to find.
            time: A time object specifying the year for which to find
                data files.
            roi: An optional geometry object limiting the search to files
                within a given geographical region.

        Return:
            A list of file records identifying the found files.
        """
        url = self.get_base_url(product) + f"/{time.year}"
        auth = accounts.get_identity("GES DISC")
        response = requests.get(url, auth=auth)
        response.raise_for_status()
        files = list(set(self.file_pattern.findall(response.text)))

        files = [f[1:-1] for f in files]
        recs = [
            FileRecord.from_remote(product, self, url + f"/{fname}", fname)
            for fname in files
        ]
        return recs


ges_disc_provider_day = GesDiscProviderDay()
ges_disc_provider_month = GesDiscProviderMonth()
ges_disc_provider_year = GesDiscProviderYear()

class GesDiscProviderBase():
    """
    Dataprovider class for for products available from the
    gpm1.gesdisc.eosdis.nasa.gov domain.
    """

    base_url = "https://gpm1.gesdisc.eosdis.nasa.gov"
    file_pattern = re.compile('"[^"]*\.(?:HDF5|h5|nc|nc4)"')

    def __init__(self, product):
        """
        Create new GesDisc provider.

        Args:
            product: The product to download.
        """
        self.product_name = str(product)
        self.level = self.product_name.split("_")[1][:2]
        super().__init__(product)

    @classmethod
    def get_available_products(cls):
        """
        Return the names of products available from this data provider.

        Return:
            A list of strings containing the names of the products that can
            be downloaded from this data provider.
        """
        return GPM_PRODUCTS.keys()

    def has_product(self, product):
        """
        Check if provider provides the given product.

        Args:
            product: A 'pansat.product' object representing the data product.

        Return:
            'True' if the given product can be downloaded through this
            provider. 'False' otherwise.
        """
        if product.name.startswith("satellite.gpm"):
            return True
        return False

    @classmethod
    def download_url(cls, url, destination):
        auth = accounts.get_identity("GES DISC")
        # If only requests.get(url, auth=auth) is used, the requests library
        # will search in ~/.netrc credentials for the machine
        # urs.earthdata.nasa.gov, but `auth` is not used as the authorization
        # is after a redirection
        # The method below handles the authorization after a redirection
        with requests.Session() as session:
            # Set credentials
            session.auth = auth

            # Get data
            redirect = session.get(url)
            response = session.get(redirect.url)

            # Write to disk
            with open(destination, "wb") as f:
                for chunk in response:
                    f.write(chunk)

    @property
    def _request_string(self):
        """The URL containing the data files for the given product."""
        base_url = "https://gpm1.gesdisc.eosdis.nasa.gov/data/"
        return base_url + GPM_PRODUCTS[str(self.product)] + "/{year}/{day}/{filename}"

    def get_files_by_year(self, year):
        """
        Return list of available files for a given day of a year.

        Args:
            year(``int``): The year for which to look up the files.

        Return:
            A list of strings containing the filenames that are available
            for the given year.
        """
        request_string = self._request_string.format(year=year, day="", filename="")
        auth = accounts.get_identity("GES DISC")
        response = requests.get(request_string, auth=auth)
        files = list(set(self.file_pattern.findall(response.text)))
        return [f[1:-1] for f in files]

    def get_files_by_month(self, year, month):
        """
        Return list of available files for a given day of a year.

        Args:
            year(``int``): The year for which to look up the files.

        Return:
            A list of strings containing the filenames that are available
            for the given year.
        """
        request_string = self._request_string.format(
            year=year, day=f"{month:02}", filename=""
        )
        auth = accounts.get_identity("GES DISC")
        response = requests.get(request_string, auth=auth)
        files = list(set(self.file_pattern.findall(response.text)))
        return [f[1:-1] for f in files]

    def get_files_by_day(self, year, day):
        """
        Return list of available files for a given day of a year.

        Args:
            year(``int``): The year for which to look up the files.
            day(``int``): The Julian day for which to look up the files.

        Return:
            A list of strings containing the filename that are available
            for the given day.
        """
        month = (
            datetime.datetime(year=year, month=1, day=1) + datetime.timedelta(days=day)
        ).month
        day = str(day)
        day = "0" * (3 - len(day)) + day
        request_string = self._request_string.format(year=year, day=day, filename="")
        auth = accounts.get_identity("GES DISC")
        response = requests.get(request_string, auth=auth)
        files = list(set(self.file_pattern.findall(response.text)))
        if len(files) == 0:
            month = f"{month:02}"
            request_string = self._request_string.format(
                year=year, day=month, filename=""
            )
            response = requests.get(request_string, auth=auth)
            files = list(set(self.file_pattern.findall(response.text)))
        return [f[1:-1] for f in files]

    def _download_with_redirect(self, url, destination):
        """
        Handles download from GES DISC server with redirect.

        Arguments:
            url(``str``): The URL of the file to retrieve.
            destination(``str``): Destination to store the data.
        """
        auth = accounts.get_identity("GES DISC")

        with requests.Session(authu=auth) as session:
            response = session.get(url, auth=auth)
            response = session.get(response.url, auth=auth, stream=True)
            response.raise_for_status()
            with open(destination, "wb") as f:
                for chunk in response:
                    f.write(chunk)

    def download_file(self, filename, destination):
        """
        Download file from data provider.

        Args:
            filename(``str``): The name of the file to download.
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """
        t = self.product.filename_to_date(filename)
        year = t.year
        day = t.strftime("%j")
        day = "0" * (3 - len(day)) + day
        if self.product.variant in ["MO"]:
            day = ""
        if self.product.variant.startswith("DAY"):
            day = f"{t.month:02}"
        url = self._request_string.format(year=year, day=day, filename=filename)
        self._download_with_redirect(url, destination)

    def download_metadata(self, filename):
        """
        Download metadata for given file.

        Args:
            filename(``str``): The name of the file to download.

        Return:
            The metadata in XML formaton
        """
        t = self.product.filename_to_date(filename)
        year = t.year
        day = t.strftime("%j")
        day = "0" * (3 - len(day)) + day
        url = (
            self._request_string.format(year=year, day=day, filename=filename) + ".xml"
        )

        response = requests.get(url)
        return ET.fromstring(response.text)
