"""
pansat.products
===============

The ``products`` module provides functionality for handling supported data products.
"""

from pansat.download.providers.copernicus import COPERNICUS_PRODUCTS
from pansat.download.providers.noaa import NOAA_PRODUCTS
from pansat.download.providers.icare import ICARE_PRODUCTS
from pansat.download.providers.ges_disc import GPM_PRODUCTS






ALL_PRODUCTS = [
    *COPERNICUS_PRODUCTS,
    *NOAA_PRODUCTS,
    *list(ICARE_PRODUCTS.keys()),
    *list(GPM_PRODUCTS.keys()),
]
