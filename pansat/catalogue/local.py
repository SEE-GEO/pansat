"""
pansat.catalogue.local
===================================
This module defines the a catalogue class to look at and list information about downloaded files in local directory.

"""


import datetime
from pansat.products.reanalysis import era5, ncep
from pansat.products.satellite import cloud_sat, era5
import glob
import os



def ProductCatalogue():
    """

    The ProductCatalogue class contains methods to extract information about
    downloaded files.
    """


    def __init__():


    def get_files(product):
        fnames = [os.path.basename(x) for x in glob.glob(product.path)]

        for f in fnames:
            product.filename_regex.match(f)

    def list_products(dir):


    def get_times(product):


    def get_variables(product):









