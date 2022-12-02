"""
pansat.catalogue.local
======================

This module defines the catalogue class to look at and list information about downloaded files in local directory.

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
            path(``str``): string or Path for product path, if None files are listed from default destination

        Returns:
            file_lists(``list``): list containing all names that have been downloaded for a certain product.
        """

        if path == None:
            path = product.default_destination
        else:
            path = Path(path)

        file_list = []

        p = path.glob("**/*")
        files = [x for x in p if x.is_file()]

        # check whether files match respective name pattern, otherwise they are not included in list
        for f in files:
            if product.filename_regexp.match(str(f.name)):
                file_list.append(f.name)

        return file_list

    def get_file_catalogue(self, destination=None):
        """
        Getting a dictionary with all downloaded files, sorted by product and product class.

        Args:

            destination(``str``): string or Path to folder to check file structure from.
                If destination is None, the catalogue starts checking for default destinations.

        Returns:

            catalogue(``dict``): nested dictionary with all downloaded files at default locations.


        """
        catalogue = {}

        if destination == None:
            for dd in DEFAULT_DESTINATIONS:
                path = Path(dd)

                if path.is_dir():
                    p = path.glob("**/*")
                    subdirectories = [x.name for x in p if x.is_dir()]
                    subdict = {}

                    for x in subdirectories:
                        subd = path / Path(x)
                        p = subd.glob("*")
                        files = [x.name for x in p if x.is_file()]
                        subdict[str(x)] = files

                    catalogue[str(path)] = subdict

        else:
            path = Path(destination)
            p = path.glob("**/*")
            subdirectories = [x.name for x in p if x.is_dir()]
            subdict = {}

            if len(subdirectories) > 0:
                for x in subdirectories:
                    subd = path / Path(x)
                    p = subd.glob("*")
                    files = [x.name for x in p if x.is_file()]
                    subdict[str(x)] = files

            else:
                subdict = {}

            catalogue[str(path)] = subdict

        return catalogue

    def print_nested(self, d, i):
        """
        Function to print a nested dictionary in tree-like structure.
        """
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
