"""
pansat.download.providers.ges_disc
==================================

This module contains a data provider for NASA's Goddard Earth Sciences Data and
Information Services Center (`GES DISC <https://disc.gsfc.nasa.gov/>`_).

Reference
---------
"""
import datetime
import json
import os
import pathlib
import re
import tempfile

import requests

from pansat.download import accounts
from pansat.download.providers.discrete_provider import DiscreteProvider

_DATA_FOLDER = pathlib.Path(__file__).parent / "data"
with open(_DATA_FOLDER / "gpm_products.json", "r") as file:
    GPM_PRODUCTS = json.load(file)

class GesdiscProvider(DiscreteProvider):
    """
    Dataprovider class for for products available from the
    gpm1.gesdisc.eosdis.nasa.gov domain.
    """

    base_url = "https://gpm1.gesdisc.eosdis.nasa.gov"
    file_pattern = re.compile('"[^"]*.HDF5"')

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

    @classmethod
    def download_url(cls, url, destination):
        auth = accounts.get_identity("GES DISC")
        r = requests.get(url, auth=auth)
        with open(destination, "wb") as f:
            for chunk in r:
                f.write(chunk)

    @property
    def _request_string(self):
        """The URL containing the data files for the given product."""
        base_url = "https://gpm1.gesdisc.eosdis.nasa.gov/data/"
        return base_url + GPM_PRODUCTS[str(self.product)] + "/{year}/{day}/{filename}"

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
        day = str(day)
        day = "0" * (3 - len(day)) + day
        request_string = self._request_string.format(year=year,
                                                     day=day,
                                                     filename="")
        auth = accounts.get_identity("GES DISC")
        response = requests.get(request_string, auth=auth)
        files = list(set(GesdiscProvider.file_pattern.findall(response.text)))
        return [f[1:-1] for f in files]

    def _download_with_redirect(self, url, destination):
        """
        Handles download from GES DISC server with redirect.

        Arguments:
            url(``str``): The URL of the file to retrieve.
            destination(``str``): Destination to store the data.
        """

        auth = accounts.get_identity("GES DISC")
        response = requests.get(url, auth=auth)
        url = response.url
        response = requests.get(url, auth=auth)

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
        url = self._request_string.format(
            year=year, day=day, filename=filename
        )

        self._download_with_redirect(url, destination)
