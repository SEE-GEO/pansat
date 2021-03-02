"""
====================================
pansat.download.providers.iowa_state
====================================

This module provides a data provider class to download MRMS products from
https://mtarchive.geol.iastate.edu/ hosted by the Department of Geological and
atmospheric sciences at Iowa State University.

"""
from datetime import datetime, timedelta
import re
import shutil

import requests

from pansat.download.providers.discrete_provider import DiscreteProvider

PRODUCTS = {
    "MRMS_PrecipRate":  ["mrms", "ncep", "PrecipRate"]
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
        date = datetime(year=year, month=1, day=1) + timedelta(days=day)
        url = self.get_url(date)
        with requests.get(url) as r:
            return self.product.filename_regexp.findall(r.text)

    def download_file(self, filename, destination):
        """
        Download file from data provider.

        Args:
            filename(``str``): The name of the file to download.
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """
        date = self.product.filename_to_date(filename)
        url = self.get_url(date)

        with requests.get(url) as r:
            with open(destination, "wb") as f:
                shutil.copyfileobj(r.raw, f)


class MRMSPrecipRate:
    def __init__(self):
        self.name = "MRMS_PrecipRate"
        self.filename_regexp = re.compile(
            "PrecipRate_00\.00_\d{8}-\d{6}.grib2\.?g?z?"
        )


    def filename_to_date(self, filename):
        name = Path(filename).name.split(".")[1]
        return datetime.strptime(name, "00_%Y%m%d-%H%M%S")

    def __str__(self):
        return "MRMS_PrecipRate"


mrms_rain_rate = MRMSPrecipRate()

provider = IowaStateProvider(mrms_rain_rate)

filename = "PrecipRate_00.00_20190411-235800.grib2.gz"
