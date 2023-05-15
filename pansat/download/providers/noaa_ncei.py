"""
pansat.download.providers.noaa_ncei
===================================

This module defines a provider for the NOAA NCEI data server containing
the GridSat brightness temperature datasets available at
 https://www.ncei.noaa.gov/data/
"""
from datetime import datetime, timedelta
import re

import requests
from pansat.download.providers.discrete_provider import DiscreteProvider


BASE_URL = "https://www.ncei.noaa.gov/data"


NCEI_PRODUCTS = {
    "gridsat_goes": "gridsat-goes/access/goes",
    "gridsat_conus": "gridsat-goes/access/conus",
    "gridsat_b1": "geostationary-ir-channel-brightness-temperature-gridsat-b1/access",
}


class NOAANCEIProvider(DiscreteProvider):
    """
       Data provider for GridSat GOES datasets  available at
    https://www.ncei.noaa.gov/data/.
    """

    def __init__(self, product):
        """
        Instantiate provider for given product.

        Args:
            product: Product instance provided by the provider.
        """
        super().__init__(product)
        self.product = product
        self.cache = {}

    @classmethod
    def get_available_products(cls):
        """
        Return the names of products available from this data provider.

        Return:
            A list of strings containing the names of the products that can
            be downloaded from this data provider.
        """
        return NCEI_PRODUCTS.keys()

    def get_files_by_month(self, year, month):
        """
        Get files available in a given month.

        Args:
            year: The year as ``int``.
            month: The month of the year as ``int``.

        Return:
            A list of the available files.
        """
        url = f"{BASE_URL}/{NCEI_PRODUCTS[self.product.name]}/{year:04}/{month:02}"
        response = requests.get(url)
        # Some products aren't split up by month. So try to get files by year
        # instead.
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            url = f"{BASE_URL}/{NCEI_PRODUCTS[self.product.name]}/{year:04}/"
            response = requests.get(url)

        pattern = re.compile(r'<a href="([^"]*\.nc)">')
        return pattern.findall(response.text)

    def get_files_by_day(self, year, day):
        """
        Get files available in a given day.

        Args:
            year: The year as ``int``.
            day: The day of the year as ``int``.

        Return:
            A list of the available files.
        """
        date = datetime(year, 1, 1) + timedelta(days=day - 1)
        month = date.month
        day_of_month = date.day

        files = self.cache.get((year, month))
        if files is None:
            files = self.get_files_by_month(year, month)
            self.cache[(year, month)] = files

        dates = map(self.product.filename_to_date, files)
        files = [
            name
            for name, date in zip(files, dates)
            if date.day == day_of_month and date.month == month
        ]
        return files

    def download_file(self, filename, destination):
        """
        Download the file to a given destination.

        Args:
            filename: Name of the file to download.
            destination: The destination to which to write the
                results.
        """
        date = self.product.filename_to_date(filename)
        year = date.year
        month = date.month

        url = f"{BASE_URL}/{NCEI_PRODUCTS[self.product.name]}/{year:04}/{month:02}/{filename}"

        response = requests.get(url)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            url = f"{BASE_URL}/{NCEI_PRODUCTS[self.product.name]}/{year:04}/{filename}"
            response = requests.get(url)
            response.raise_for_status()

        with open(destination, "wb") as output:
            for chunk in response:
                output.write(chunk)
