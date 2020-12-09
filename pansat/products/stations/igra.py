"""
pansat.products.soundings.igra
=======================================

This module defines the IGRA product class, which represents the global data product of radiosoundings: IGRA.


"""

import re
import os
from datetime import datetime, timedelta
from pathlib import Path
import pansat.download.providers as providers
from pansat.products.product import Product
import pandas as pd
from math import radians, cos, sin, asin, sqrt


class NoAvailableProviderError(Exception):
    """
    Exception indicating that no suitable provider could be found for
    a product.
    """


class IGRASoundings(Product):
    """
    The IGRA reanalysis class defines a generic interface for IGRA products.

    Attributes:
        name(``str``): name of product
        locs(pd.DataFrame): pandas dataframe with metadata on stations
                            (contains location ID, coordinates, and time period)

        station(``pd.DataFrame``): pandas dataframe with metadata for chosen station
        variable(``str`` ): variable to extract, if no station is given
    """

    def __init__(self, location=None, variable=None):
        """
        Args:

        location(``str`` or tuple): station name or tuple with closest coordinates as float or int (lat,lon)

        variable(``str`` ): variable to extract, if given monthly data of all stations will be downloaded

        -- available variables:--
        ghgt = Geopotential height
        temp = Temperature
        uwnd = Zonal wind component
        vapr = Vapor pressure
        vwnd = Meridional wind component
        -----------------------------------
        """
        self.name = "igra-soundings"
        provider = self._get_provider()
        provider = provider(self)

        destination = self.default_destination
        destination.mkdir(parents=True, exist_ok=True)
        # download meta data of all locations
        downloaded = provider.download(
            start=0,
            end=0,
            destination=destination,
            base_url="ftp.ncdc.noaa.gov",
            product_path="/pub/data/igra/",
            files=["igra2-station-list.txt"],
        )

        self.variable = variable
        self.station = location

        # pandas data frame with all locations and meta information
        self.locs = self.get_metadata()

        # define column names of pandas dataframe with station info
        colnames = ["ID", "lat", "lon", "n", "name", "start", "end", "nr"]
        self.locs.columns = colnames

        if self.station == None:
            self.filename_regexp = re.compile(str(self.variable) + ".*" + r".txt.zip")

        else:
            if isinstance(location, str):
                self.station = self.locs[self.locs.name == location]
            else:
                self.station = self.locs[
                    self.locs.name == self.find_nearest(location[0], location[1])
                ]
                self.filename_regexp = re.compile(
                    str(self.station.ID.values[0]) + ".*" + r".txt.zip"
                )

    def get_metadata(self):
        """Extracts data from meta data station inventory."""
        locs = pd.read_fwf(
            str(self.default_destination) + "/igra2-station-list.txt",
            sep="/s",
            engine="python",
            header=None,
        )
        return locs

    def dist(self, lat1, lon1, lat2, lon2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        # convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        # Radius of earth in kilometers is 6371
        km = 6371 * c
        return km

    def find_nearest(self, lat, lon):
        """Find location of closest station to a given set of coordinates.  """
        distances = self.locs.apply(
            lambda row: self.dist(lat, lon, row["lat"], row["lon"]), axis=1
        )
        return self.locs.loc[distances.idxmin(), "name"]

    def matches(self, filename):
        """
        Determines whether a given filename matches the pattern used for
        the product.
        Args:
            filename(``str``): The filename
        Return:
            True if the filename matches the product, False otherwise.
        """
        return self.filename_regexp.match(filename)

    def filename_to_date(self, filename):
        """
        Extract timestamp from filename.
        Args:
            filename(``str``): Filename of a NCEP product.
        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """
        filename = os.path.basename(filename)
        filename = filename.split(".")[-2]
        pattern = "%Y"

        return datetime.strptime(filename, pattern)

    def _get_provider(self):
        """ Find a provider that provides the product. """
        available_providers = [
            p
            for p in providers.ALL_PROVIDERS
            if str(self) in p.get_available_products()
        ]
        if not available_providers:
            raise NoAvailableProviderError(
                f"Could not find provider for the product {self.name}."
            )
        return available_providers[0]

    @property
    def default_destination(self):
        """
        The default destination for IGRA product is
        ``IGRA/<product_name>``>
        """
        return Path("IGRA") / Path(self.name)

    def __str__(self):
        """ The full product name. """
        return self.name

    def get_filename(self, product_path):
        """Get filename for specific station

        Returns:
        filename(str): filename for download
        """

        if self.variable != None:
            fname = [
                self.variable + "_00z-mly.txt.zip",
                self.variable + "_12z-mly.txt.zip",
            ]

        elif "2yd" in product_path:
            fname = [str(self.station["ID"].values[0]) + "-data-beg2018.txt.zip"]

        else:
            fname = [str(self.station["ID"].values[0]) + "-data.txt.zip"]

        return fname

    def download(self, period=None, destination=None, provider=None):
        """
        Download IGRA sounding data for a given station.

        Args:

        period(``str``): 'recent' to download only past 1-2 years instead of full period (last month for monthly data)
        destination(``str`` or ``pathlib.Path``): The destination where to store
                 the output data.
        Returns:

        downloaded(``list``): ``list`` with names of all downloaded files for respective data product

        """
        if self.variable != None:
            path = "/pub/data/igra/monthly/monthly-"
            if period == "recent":
                product_path = path + "upd/"
            else:
                product_path = path + "por/"
        else:
            path = "/pub/data/igra/data/data-"
            if period == "recent":
                product_path = path + "2yd/"
            else:
                product_path = path + "por/"

        if not provider:
            provider = self._get_provider()
        if not destination:
            destination = self.default_destination
        else:
            destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)

        provider = provider(self)
        filename = self.get_filename(product_path)

        downloaded = provider.download(
            start=0,
            end=0,
            destination=destination,
            base_url="ftp.ncdc.noaa.gov",
            product_path=product_path,
            files=filename,
        )

        return downloaded
