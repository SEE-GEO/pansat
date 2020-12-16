"""
pansat.download.providers.ges_disc
==================================

This module contains a data provider for NASA's Goddard Earth Sciences Data and
Information Services Center (GES DISC).

Reference
---------
"""
import datetime

from pansat.download import accounts
from pansat.download.providers.discrete_provider import DiscreteProvider
import requests
import re

LAADS_PRODUCTS = [
    "MODIS_Terra_MOD021KM",
    "MODIS_Terra_MOD03",
    "MODIS_Terra_MOD35_l2",
    "MODIS_Aqua_MYD021KM",
    "MODIS_Aqua_MYD03",
    "MODIS_Aqua_MYD35_l2",
]


class LAADSDACProvider(DiscreteProvider):
    """
    Dataprovider class for for products available from the
    gpm1.gesdisc.eosdis.nasa.gov domain.
    """

    base_url = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/"
    file_pattern = re.compile('[\w\.]*.hdf')

    def __init__(self, product):
        """
        Create new GesDisc provider.

        Args:
            product: The product to download.
        """
        super().__init__(product)

    @classmethod
    def get_available_products(cls):
        """
        Return the names of products available from this data provider.

        Return:
            A list of strings containing the names of the products that can
            be downloaded from this data provider.
        """
        return LAADS_PRODUCTS

    @property
    def _request_string(self):
        """The URL containing the data files for the given product."""
        base_url = LAADSDACProvider.base_url
        base_url += f"{self.product.product_name.upper()}"
        return base_url + "/{year}/{day}/{filename}"

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
        request_string = self._request_string.format(year=year, day=day, filename="")
        response = requests.get(request_string)
        files = list(set(LAADSDACProvider.file_pattern.findall(response.text)))
        return [f for f in files]

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
        request_string = self._request_string.format(
            year=year, day=day, filename=filename
        )
        print("downloading:", request_string)
        auth = accounts.get_identity("GES DISC")
        r = requests.get(request_string, auth=auth)
        with open(destination, "wb") as f:
            for chunk in r:
                f.write(chunk)
