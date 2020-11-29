
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
        locations(``dict``): metadata on stations as dictionary with names as keys
        station(``pd.DataFrame``): pandas dataframe with metadata for chosen station  
    """
    def __init__(self, location):
        """
        Args:

        location(``str`` or tuple): station name or tuple closest coordinates (lat,lon)
       """

        # download meta data of all locations
        self.name = 'igra-soundings'

        if not provider:
            provider = self._get_provider()
        downloaded = provider.download(t0= None,t1=None ,destination, url = 'ftp.ncdc.noaa.gov', files = list('igra2-station-lsit.txt'))

        # dictionary with all locations and meta information
        self.locs = self.get_metadata(locations)

        if isinstance(location,str):
            self.station = locs[locs['name'] == location]
        else:
            self.station = locs[locs.name == find_nearest(location[0], location[1])]


    def get_metadata(self):
        """Extracts data from meta data station inventory."""
        columns = ['ID', 'lat', 'lon', 'n', 'name', 'start', 'end', 'nr']
        locs= pd.read_fwf('igra2-station-lsit.txt', sep='/s', engine = 'python', header = None, columns =columns)
        return locs


    def dist(self,lat1, lon1, lat2,lon2):
        """
        Calculate the great circle distance between two points 
        on the earth (specified in decimal degrees)
        """
        # convert decimal degrees to radians 
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        # haversine formula 
        dlon = lon2 - lon1 
        dlat = lat2 - lat1 
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a)) 
        # Radius of earth in kilometers is 6371
        km = 6371* c
        return km


    def find_nearest(self,lat,lon):
        """Find location of closest station to a given set of coordinates.  """
        distances = locs.apply(
            lambda row: self.dist(lat, lon, row['lat'], row['lon']), 
            axis=1)
        return locs.loc[distances.idxmin(), 'name']


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
        fname = str(self.station['ID']) + '-data.txt.zip'
        if '2yd' in product_path:
            str(self.station['ID']) + '-data-beg2018.txt.zip'
        return fname





    def download(self, resolution, period, destination=None, provider= None ):
        """
        Download IGRA sounding data for a given station.

        Args:
        resolution(``str``): 'monthly' for monthly averages or 'original' for original timesteps  
        period(``str``): 'recent' to download only past 1-2 years or 'full' for full period will be downloaded
        destination(``str`` or ``pathlib.Path``): The destination where to store
                 the output data.
        Returns:

        downloaded(``list``): ``list`` with names of all downloaded files for respective data product

        """
        if resolution == 'monthly':
            path = 'igra-data-'
        else:
            path = 'igra-monthly-'

        if period == 'recent':
            self.product_path = path + 'data-y2d'
        else:
            self.product_path = path+ 'data-por'


        if not provider:
            provider = self._get_provider()
        if not destination:
            destination = self.default_destination
        else:
            destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)

        provider = provider(self)
        filename= self.get_filename(product_path)

        downloaded = provider.download(t0= None, t1= None, destination, url = 'ftp.ncdc.noaa.gov', product_path, files = list(filename))

        return downloaded


