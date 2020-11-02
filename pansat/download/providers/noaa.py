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
    def __init__(self, product):
        """
        Create a new product instance.

        Args:

        product(str): product name
        """
        super().__init__()
        self.product = product

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

    @abstractmethod
    def download(self, start, end, destination=None):
        """
        This method downloads data for a given time range from respective the
        data provider.

        Args:
            start(``datetime.datetime``): date and time for start
            end(``datetime.datetime``): date and time for end
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """


        path  = Path('ftp://ftp2.psl.noaa.gov/Datasets/ncep.reanalysis/surface/') 
        ftp = ftplib.FTP(path)
        ftp.login("anonymous", mailpw)
        ftp.cwd(targetdir)

        with open(filename, 'wb') as fp:
            ftp.retrbinary('RETR ' + filename, fp.write)
            ft.quit()

















