"""
pansat.download.providers
=========================

The ``providers`` module provides classes to access online services that allow
downloading data. Objects of these data provider classes provide download access
for specific data products. For this,  an object of the data provider class
must be instantiated with a given product object. If the product is available
from the data provider class, the resulting data provider instance can be used
to download files of the data product.

Example
-------

.. code-block::

    from datetime import datetime
    from pansat.download.provider.icare import IcareProvider
    from pansat.products.satellite.cloud_sat import l1b_cpr

    provider = IcareProvider(l1b_cpr)
    t_0 = datetime(2016, 11, 21, 10)
    t_1 = datetime(2016, 11, 21, 12)
    files = provider.download(t_0, t_1)

Attributes:
    ALL_PROVIDERS: List containing all available data provider classes.
        This list should be used by products to determine a data provider to
        use to download files.
"""
from pansat.download.providers.data_provider import DataProvider
from pansat.download.providers.copernicus import CopernicusProvider
from pansat.download.providers.icare import IcareProvider
from pansat.download.providers.ges_disc import GesdiscProvider, Disc2Provider
from pansat.download.providers.noaa import NOAAProvider
from pansat.download.providers.meteo_france import GeoservicesProvider
from pansat.download.providers.laads_daac import LAADSDAACProvider
from pansat.download.providers.goes_aws import GOESAWSProvider
from pansat.download.providers.himawari_aws import HimawariAWSProvider
from pansat.download.providers.iowa_state import IowaStateProvider
from pansat.download.providers.eumetsat import EUMETSATProvider
from pansat.download.providers.uci import UciProvider
from pansat.download.providers.noaa_ncei import NOAANCEIProvider
from pansat.download.providers.cloudnet import CloudnetProvider
from pansat.download.providers.cloudsat_dpc import CloudSatDPCProvider

ALL_PROVIDERS = [
    CopernicusProvider,
    IcareProvider,
    GesdiscProvider,
    Disc2Provider,
    NOAAProvider,
    GeoservicesProvider,
    LAADSDAACProvider,
    GOESAWSProvider,
    HimawariAWSProvider,
    IowaStateProvider,
    EUMETSATProvider,
    UciProvider,
    NOAANCEIProvider,
    CloudnetProvider,
    CloudSatDPCProvider
]
