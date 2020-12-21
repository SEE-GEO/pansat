"""
pansat.catalogue.local
===================================
This module defines the a catalogue class to look at and list information about downloaded files in local directory.

"""


import datetime
import glob
import os
from pathlib import Path
from pansat.products import ALL_PRODUCTS
from pansat.products.reanalysis import era5, ncep
from pansat.products.satellite import cloud_sat, calipso, dardar, gpm
from pansat.products.ground_based import opera


DEFAULT_DESTINATIONS = [
    "ERA5",
    "NCEP",
    "IGRA",
    "Calipso",
    "Cloudsat",
    "Dardar",
    "GPM",
    "Opera",
]


class ProductCatalogue:
    """

    The ProductCatalogue class contains methods to extract information about
    downloaded files.

    Attributes:
    available_products: string list with all currently supoprted products

    """

    def __init__(self):
        self.available_products = ALL_PRODUCTS

    def get_files_for_product(self, product, path=None):
        """

        Get list with all files for specific product.

        Args:
        product: pansat product instance
        paths(``PosixPath object``): product path, if None files are listed from default destination

        Returns:
        file_lists(``list``):
        """

        if path == None:
            path = product.default_destination
        file_list = []

        p = path.glob("**/*")
        files = [x for x in p if x.is_file()]

        # check whether files match respective name pattern, otherwise they are not included in list
        for f in files:
            if product.filename_regexp.match(str(f.name)):
                file_list.append(f.name)

        return file_list

    def get_file_catalogue(self):
        """
        Getting a dictionary with all downloaded files, sorted by product and product class.

        Returns:

        catalogue(``dict``): nested dictionary with all downloaded files at default locations.


        """
        catalogue = {}
        for dd in DEFAULT_DESTINATIONS:
            path = Path(dd)

            if path.is_dir():
                p = path.glob("**/*")
                subdirectories = [x.name for x in p if x.is_dir()]
                p = path.glob("**/*")
                files = [x.name for x in p if x.is_file()]
                subdict = {}

                for idx, x in enumerate(subdirectories):
                    subdict[str(x)] = files[idx]

                catalogue[str(path)] = subdict

        return catalogue

    def print_nested(self, d, i):
        for key, value in d.items():
            print("\t" * i + str(key))
            if isinstance(value, dict):
                self.print_nested(value, i + 1)
            else:
                print("\t" * (i + 1) + str(value))

    def print_file_catalogue(self):
        """

        Prints the nested dictionary with all files per product and product class in tree-like structure.

        """
        catalogue = self.get_file_catalogue()
        self.print_nested(catalogue, 0)
