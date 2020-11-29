
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



class NoAvailableProviderError(Exception):
    """
    Exception indicating that no suitable provider could be found for
    a product.
    """


class IGRASoundings(Product):
    """
    The IGRA reanalysis class defines a generic interface for IGRA products.

    Attributes:
        variable(``str``): Variable to extract
        grid(``str``): pressure, surface, spectral, surface_gauss or tropopause
        name(``str``): Full name of the product.
    """


    def __init__(self, location, resolution = None ):
        """
        Args:

        location(``str``): location ID, name or closest coordinates 
        resolution(``str``): 'monthly' for monthly averages, otherwise all timesteps will be downloaded 
        """

        if resolution == 'monthly':
            self.product_path = '/pub/data/igra/data/'
        else:
            self.product_path = '/pub/data/igra/monthly/'


        # download meta data of all locations 
        downloaded = provider.download(t0= None,t1=None ,destination, url = 'ftp.ncdc.noaa.gov', '/pub/data/igra', files = list('igra2-station-lsit.txt'))


        # dictionary with all locations and meta information
        self.locations = locations

        self.location,self.id_loc,self.coords = get_location(self,lat,lon)




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
            filename(``str``): Filename of a IGRA product.
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


    def get_location(self, lat, lon):
        """ Find closest station of global dataset.

        Args:

        lat(float): latitude value
        lon(float): longitude value

        Returns:

        location: location name 
        id: location ID 
        coords: lat and lon value of location 

        """
        pass


    def get_filename(self, coords):
        """Get filename for specific station



        Returns:
        filename(str): filename for download 


        """
        pass











    def download(self, t0, t1, destination=None, provider  = None ):
        """
        Download data product for given time range.

        Args:
            start_time(``datetime``): ``datetime`` object defining the start date
            end_time(``datetime``): ``datetime`` object defining the end date
            destination(``str`` or ``pathlib.Path``): The destination where to store
                 the output data.

        Returns:

        downloaded(``list``): ``list`` with names of all downloaded files for respective data product

        """
        if not provider:
            provider = self._get_provider()
        if not destination:
            destination = self.default_destination
        else:
            destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        provider = provider(self)


        filename= get_filename(self, self.coords)

        downloaded = provider.download(t0= None, t1= None, destination, url = 'ftp.ncdc.noaa.gov', self.product_path, files = list(filename))

        return downloaded

