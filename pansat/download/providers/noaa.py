"""
pansat.download.providers.noaa
=======================================

This module provides the NoaaProvider class to download data stored at the NOAA data server.


"""


from contextlib import contextmanager
import itertools
import os
from pathlib import Path
import tempfile
import numpy as np
from pansat.download.accounts import get_identity
from pansat.download.providers.data_provider import DataProvider
from datetime import datetime, timedelta
import ftplib



NOAA_PRODUCTS = ["ncep.reanalysis-surface", "ncep.reanalysis-pressure", "ncep.reanalysis-surface_gauss", "ncep.reanalysis-spectral", "ncep.reanalysis-tropopause"]


class NOAAProvider(DataProvider):
    """
    Abstract base class for gridded products available from NOAA Physical Science Laboratory.
    """

    base_url = "ftp2.psl.noaa.gov"

    def __init__(self, product):
        """
        Create a new product instance.

        Args:

        product: Product class object with specific product for NOAA
        """
        super().__init__()
        self.product = product
        self.product_path = "/Datasets/" + ("/").join(self.product.name.split("-"))
        self.cache = {}

        if not product.name in NOAA_PRODUCTS:
            available_products = NOAA_PRODUCTS
            raise ValueError(
                f"{product.name} not a available from the NOAA data"
                " provider. Currently available products are: "
                f" {available_products}."
            )

    @classmethod
    def get_available_products(cls):
        """
        Return the names of products available from this data provider.

        Return:
            A list of strings containing the names of the products that can
            be downloaded from this data provider.
        """
        return NOAA_PRODUCTS

    def _ftp_listing_to_list(self, path, item_type=int):
        """
        Retrieve directory content from ftp listing as list.

        Args:

           path(str): The path from which to retrieve the ftp listing.

           t(type): Type constructor to apply to the elements of the
           listing. To retrieve a list of strings use t = str.

        Return:

            A list containing the content of the ftp directory.

        """
        if not path in self.cache:
            with FTP(NOAAProvider.base_url) as ftp:
                user, password = get_identity("NOAAProvider")
                ftp.login(user=user, passwd=password)
                try:
                    ftp.cwd(path)
                except:
                    raise Exception(
                        "Can't find product folder "
                        + path
                        + "on the NOAA server. Are you sure this is the right path?"
                    )
                listing = ftp.nlst()
            listing = [item_type(l) for l in listing]
            self.cache[path] = listing
        return self.cache[path]

    def get_file_names(self, var, start, end):
        """
        Return all files from given year and julian day.

        Args:
            var(``str``): Variable to extract
            start(``int``): start year for desired time range
            end(``int``): end year for desired timerange
        Return:
            List of the filenames of this product for given variable and time range by year."""

        files = []
        for y in np.arange(start, end + 1):
            year_str = str(y)
            fn = var + "." + year_str + ".nc"
            files.append(fn)
        return files


    def download(self, start, end, destination):
        """
        This method downloads data for a given time range from respective the
        data provider.

        Args:
            start(``int``): start year
            end(``int``): end year
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """

        # get file list
        files = self.get_file_names(self.product.variable, start, end)

        ftp = ftplib.FTP(self.base_url)
        user, password = get_identity("NOAAProvider")
        ftp.login(user=user, passwd=password)
        ftp.cwd(self.product_path)

        for filename in files:
            output = Path(str(destination)) / str(filename)
            with open(str(output), "wb") as fp:
                ftp.retrbinary("RETR " + filename, fp.write)

        ftp.quit()
        return files 
