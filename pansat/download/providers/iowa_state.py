"""
====================================
pansat.download.providers.iowa_state
====================================

This module provides a data provider class to download MRMS products from an
archive located at https://mtarchive.geol.iastate.edu/, which is hosted by the
Department of Geological and atmospheric sciences at Iowa State University.

This provider doesn't require any user authentication to download data.
"""
from datetime import datetime, timedelta
import shutil

import requests

from pansat.download.providers.discrete_provider import DiscreteProvider

PRODUCTS = {
    "MRMS_PrecipRate": ["mrms", "ncep", "PrecipRate"],
    "MRMS_RadarQualityIndex": ["mrms", "ncep", "RadarQualityIndex"],
    "MRMS_PrecipFlag": ["mrms", "ncep", "PrecipFlag"],
    "MRMS_SeamlessHSR": ["mrms", "ncep", "SeamlessHSR"],
}


class IowaStateProvider(DiscreteProvider):
    """
    Base class for data products available from htps://mtarchive.geol.iastate.edu/
    """

    base_url = "https://mtarchive.geol.iastate.edu/"

    def __init__(self, product):
        """
        Create a new product instance.

        Args:

            product(``Product``): Product class object available from the archive.

        """
        if str(product) not in PRODUCTS:
            available_products = list(PRODUCTS.keys())
            raise ValueError(
                f"The product {product} is  not a available from the Iowa State"
                f"archive provider. Currently available products are: "
                f"{available_products}."
            )
        super().__init__(product)
        self.url_suffix = "/".join(PRODUCTS[str(product)])

    @classmethod
    def get_available_products(cls):
        return PRODUCTS.keys()

    def get_url(self, date):
        url = f"{self.base_url}/{date.year}/{date.month:02}/{date.day:02}/"
        url += self.url_suffix
        return url

    def get_files_by_day(self, year, day):
        """
        Return all files from given year and julian day.

        Args:
            year(``int``): The year from which to retrieve the filenames.
            day(``int``): Day of the year of the data from which to retrieve the
                the filenames.

        Return:
            List of the filenames of this product on the given day.
        """
        date = datetime(year=year, month=1, day=1) + timedelta(days=day - 1)
        url = self.get_url(date)
        with requests.get(url) as r:
            files_unique = set(self.product.filename_regexp.findall(r.text))
            return sorted(list(files_unique))

    def download_file(self, filename, destination):
        """
        Download file from data provider.

        Args:
            filename(``str``): The name of the file to download.
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """
        date = self.product.filename_to_date(filename)
        url = self.get_url(date) + "/" + filename

        with requests.get(url, stream=True) as r:
            with open(destination, "wb") as f:
                shutil.copyfileobj(r.raw, f)
