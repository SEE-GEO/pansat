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
                user, password = get_identity("NOAA")
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


    def get_files_names(self,var,year):
        """
        Return all files from given year and julian day.

        Args:
            var(``str``): Variable to extract
            start(``int``): start year for desired time range 
            end(``int``): end year for desired timerange 
        Return:
            List of the filenames of this product for given variable and time range by year. """

        files = []
        for y in np.arange(start,end + 1):
            year_str = str(y)
            fn= var+ "_" + y + ."nc"
            path = "/".join([self.product_path, fn])
            files.append(path)
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
        user, password = get_identity("NOAA")
        ftp.login(user = user, passwd= password)
        ftp.cwd(path)

        with open(destination, 'wb') as fp:
            ftp.retrbinary('RETR ' + filename, fp.write)
            ft.quit()
















