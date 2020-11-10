"""
pansat.products.reanalysis.ncep
===================================
This module defines the NCEP reanalysis product class, which represents all
supported NCEP reanalysis products.


"""




class NoAvailableProviderError(Exception):
    """
    Exception indicating that no suitable provider could be found for
    a product.
    """




class NCEPSurface(Product):
    """
    The NCEP reanalysis class defines a generic interface for ERA5 products.

    Attributes:
        variable(``str``): Variable to extract 
    """

    def __init__(self, variable):
        self.variable = variable










