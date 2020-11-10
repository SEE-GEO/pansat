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


NOAA_PRODUCTS = ['ncep-reanalysis-surface, ncep-reanalysis-pressure']


class NOAAProvider(DataProvider):
    """
    Abstract base class for gridded products available from NOAA Physical Science Laboratory.
    """
    base_url = "ftp://ftp2.psl.noaa.gov/Datasets"

    def __init__(self, product):
        """
        Create a new product instance.

        Args:

        product_path(str): The path of the product. This should point to
        the folder that holds yearly .nc files for different variables. 
        """
        super().__init__()
        self.product_path = str(product_path)

        if not product.name in NOAA_PRODUCTS:
            available_products = NOAA_PRODUCTS
            raise ValueError(
                f"{product.name} not a available from the Copernicus data"
                " provider. Currently available products are: "
                f" {available_products}."
            )


    @abstractclassmethod
    def get_available_products(cls):
        """
        Return the names of products available from this data provider.

        Return:
            A list of strings containing the names of the products that can
            be downloaded from this data provider.
        """


    def get_files_by_year(self, year, day):
        """
        Return all files from given year and julian day.

        Args:
            year(``int``): The year from which to retrieve the filenames.
   
        Return:
            List of the filenames of this product on the given day. """


        return files



    @abstractmethod
    def download(self, filename, destination=None):
        """
        This method downloads data for a given time range from respective the
        data provider.

        Args:
            filename(``str``): The name of the file to download.
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """

        # target directory
        path = "/".join([self.base_url, self.product_path])

        ftp = ftplib.FTP(path)
        ftp.login(user = "anonymous", passwd= mailpw)
        ftp.cwd(path)

        with open(destination, 'wb') as fp:
            ftp.retrbinary('RETR ' + filename, fp.write)
            ft.quit()
















